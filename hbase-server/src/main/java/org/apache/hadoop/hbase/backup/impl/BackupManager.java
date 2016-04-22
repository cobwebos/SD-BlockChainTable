/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.master.BackupController;
import org.apache.hadoop.hbase.backup.master.BackupLogCleaner;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.regionserver.LogRollRegionServerProcedureManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Handles backup requests on server-side, creates backup context records in hbase:backup
 * to keep track backup. The timestamps kept in hbase:backup table will be used for future
 * incremental backup. Creates BackupContext and DispatchRequest.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupManager implements Closeable {
  private static final Log LOG = LogFactory.getLog(BackupManager.class);

  private Configuration conf = null;
  private BackupInfo backupContext = null;

  private ExecutorService pool = null;

  private boolean backupComplete = false;

  private BackupSystemTable systemTable;

  private final Connection conn;

  /**
   * Backup manager constructor.
   * @param conf configuration
   * @throws IOException exception
   */
  public BackupManager(Configuration conf) throws IOException {
    if (!conf.getBoolean(HConstants.BACKUP_ENABLE_KEY, HConstants.BACKUP_ENABLE_DEFAULT)) {
      throw new BackupException("HBase backup is not enabled. Check your " +
          HConstants.BACKUP_ENABLE_KEY + " setting.");
    }
    this.conf = conf;
    this.conn = ConnectionFactory.createConnection(conf);
    this.systemTable = new BackupSystemTable(conn);
     
    Runtime.getRuntime().addShutdownHook(new ExitHandler());

  }

  /**
   * Return backup context
   */
  protected BackupInfo getBackupContext()
  {
    return backupContext;
  }
  /**
   * This method modifies the master's configuration in order to inject backup-related features
   * @param conf configuration
   */
  public static void decorateMasterConfiguration(Configuration conf) {
    if (!isBackupEnabled(conf)) {
      return;
    }
    // Add WAL archive cleaner plug-in
    String plugins = conf.get(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    String cleanerClass = BackupLogCleaner.class.getCanonicalName();
    if (!plugins.contains(cleanerClass)) {
      conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, plugins + "," + cleanerClass);
    }    
    
    String classes = conf.get("hbase.procedure.master.classes");
    String masterProcedureClass = LogRollMasterProcedureManager.class.getName();
    if(classes == null){    
      conf.set("hbase.procedure.master.classes", masterProcedureClass);
    } else if(!classes.contains(masterProcedureClass)){
      conf.set("hbase.procedure.master.classes", classes +","+masterProcedureClass);
    }    
 
    // Set Master Observer - Backup Controller
    classes = conf.get("hbase.coprocessor.master.classes");
    String observerClass = BackupController.class.getName();
    if(classes == null){    
      conf.set("hbase.coprocessor.master.classes", observerClass);
    } else if(!classes.contains(observerClass)){
      conf.set("hbase.coprocessor.master.classes", classes +","+observerClass);
    }    

    if (LOG.isDebugEnabled()) {
      LOG.debug("Added log cleaner: " + cleanerClass);
      LOG.debug("Added master procedure manager: "+masterProcedureClass);
      LOG.debug("Added master observer: "+observerClass);      
    }
    
  }

  /**
   * This method modifies the RS configuration in order to inject backup-related features
   * @param conf configuration
   */
  public static void decorateRSConfiguration(Configuration conf) {
    if (!isBackupEnabled(conf)) {
      return;
    }
    
    String classes = conf.get("hbase.procedure.regionserver.classes");
    String regionProcedureClass = LogRollRegionServerProcedureManager.class.getName();
    if(classes == null){    
      conf.set("hbase.procedure.regionserver.classes", regionProcedureClass);
    } else if(!classes.contains(regionProcedureClass)){
      conf.set("hbase.procedure.regionserver.classes", classes +","+regionProcedureClass);
    }    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added region procedure manager: "+regionProcedureClass);
    }
    
  }
  
  
  private static boolean isBackupEnabled(Configuration conf) {
    return conf.getBoolean(HConstants.BACKUP_ENABLE_KEY, HConstants.BACKUP_ENABLE_DEFAULT);
  }

  // TODO: remove this on the server side
  private class ExitHandler extends Thread {
    public ExitHandler() {
      super("Backup Manager Exit Handler");
    }

    @Override
    public void run() {
      if (backupContext != null && !backupComplete) {

        // program exit and backup is not complete, then mark as cancelled to avoid submitted backup
        // handler's taking further action
        backupContext.setCancelled(true);

        LOG.debug("Backup is cancelled due to force program exiting.");
        try {
          cancelBackup(backupContext.getBackupId());
        } catch (Exception e) {
          String msg = e.getMessage();
          if (msg == null || msg.equals("")) {
            msg = e.getClass().getName();
          }
          LOG.error("Failed to cancel backup " + backupContext.getBackupId() + " due to " + msg);
        }
      }
      close();
    }
  }

  /**
   * Get configuration
   * @return configuration
   */
  Configuration getConf() {
    return conf;
  }

  /**
   * Cancel the ongoing backup via backup id.
   * @param backupId The id of the ongoing backup to be cancelled
   * @throws Exception exception
   */
  private void cancelBackup(String backupId) throws Exception {
    // TODO: will be implemented in Phase 2: HBASE-14125
    LOG.debug("Try to cancel the backup " + backupId + ". the feature is NOT implemented yet");

  }

  /**
   * Stop all the work of backup.
   */
  @Override
  public void close() {
    // currently, we shutdown now for all ongoing back handlers, we may need to do something like
    // record the failed list somewhere later
    if (this.pool != null) {
      this.pool.shutdownNow();
    }
    if (systemTable != null) {
      try {
        systemTable.close();
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    if (conn != null) {
      try {
        conn.close();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
  }

  /**
   * Create a BackupContext based on input backup request.
   * @param backupId backup id
   * @param type    type
   * @param tablelist table list
   * @param targetRootDir root dir
   * @param snapshot snapshot name
   * @return BackupContext context
   * @throws BackupException exception
   */
  public BackupInfo createBackupContext(String backupId, BackupType type,
      List<TableName> tableList, String targetRootDir, int workers, long bandwidth)
          throws BackupException {
    if (targetRootDir == null) {
      throw new BackupException("Wrong backup request parameter: target backup root directory");
    }

    if (type == BackupType.FULL && (tableList == null || tableList.isEmpty())) {
      // If table list is null for full backup, which means backup all tables. Then fill the table
      // list with all user tables from meta. It no table available, throw the request exception.

      HTableDescriptor[] htds = null;
      try (Admin hbadmin = conn.getAdmin()) {
        htds = hbadmin.listTables();
      } catch (Exception e) {
        throw new BackupException(e);
      }

      if (htds == null) {
        throw new BackupException("No table exists for full backup of all tables.");
      } else {
        tableList = new ArrayList<>();
        for (HTableDescriptor hTableDescriptor : htds) {
          tableList.add(hTableDescriptor.getTableName());
        }

        LOG.info("Full backup all the tables available in the cluster: " + tableList);
      }
    }

    // there are one or more tables in the table list
    backupContext = new BackupInfo(backupId, type, 
      tableList.toArray(new TableName[tableList.size()]),
      targetRootDir);
    backupContext.setBandwidth(bandwidth);
    backupContext.setWorkers(workers);
    return backupContext;
  }

  /**
   * Check if any ongoing backup. Currently, we only reply on checking status in hbase:backup. We
   * need to consider to handle the case of orphan records in the future. Otherwise, all the coming
   * request will fail.
   * @return the ongoing backup id if on going backup exists, otherwise null
   * @throws IOException exception
   */
  private String getOngoingBackupId() throws IOException {

    ArrayList<BackupInfo> sessions = systemTable.getBackupContexts(BackupState.RUNNING);
    if (sessions.size() == 0) {
      return null;
    }
    return sessions.get(0).getBackupId();
  }

  /**
   * Start the backup manager service.
   * @throws IOException exception
   */
  public void initialize() throws IOException {
    String ongoingBackupId = this.getOngoingBackupId();
    if (ongoingBackupId != null) {
      LOG.info("There is a ongoing backup " + ongoingBackupId
          + ". Can not launch new backup until no ongoing backup remains.");
      throw new BackupException("There is ongoing backup.");
    }

    // Initialize thread pools
    int nrThreads = this.conf.getInt("hbase.backup.threads.max", 1);
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("BackupHandler-%1$d");
    this.pool =
        new ThreadPoolExecutor(nrThreads, nrThreads, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), builder.build());
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
  }

  public void setBackupContext(BackupInfo backupContext) {
    this.backupContext = backupContext;
  }

  /**
   * Get direct ancestors of the current backup.
   * @param backupCtx The backup context for the current backup
   * @return The ancestors for the current backup
   * @throws IOException exception
   * @throws BackupException exception
   */
  public ArrayList<BackupImage> getAncestors(BackupInfo backupCtx) throws IOException,
      BackupException {
    LOG.debug("Getting the direct ancestors of the current backup "+ 
      backupCtx.getBackupId());

    ArrayList<BackupImage> ancestors = new ArrayList<BackupImage>();

    // full backup does not have ancestor
    if (backupCtx.getType() == BackupType.FULL) {
      LOG.debug("Current backup is a full backup, no direct ancestor for it.");
      return ancestors;
    }

    // get all backup history list in descending order

    ArrayList<BackupInfo> allHistoryList = getBackupHistory(true);
    for (BackupInfo backup : allHistoryList) {
      BackupImage image =
          new BackupImage(backup.getBackupId(), backup.getType(),
            backup.getTargetRootDir(),
              backup.getTableNames(), backup.getStartTs(), backup
                  .getEndTs());
      // add the full backup image as an ancestor until the last incremental backup
      if (backup.getType().equals(BackupType.FULL)) {
        // check the backup image coverage, if previous image could be covered by the newer ones,
        // then no need to add
        if (!BackupManifest.canCoverImage(ancestors, image)) {
          ancestors.add(image);
        }
      } else {
        // found last incremental backup, if previously added full backup ancestor images can cover
        // it, then this incremental ancestor is not the dependent of the current incremental
        // backup, that is to say, this is the backup scope boundary of current table set.
        // Otherwise, this incremental backup ancestor is the dependent ancestor of the ongoing
        // incremental backup
        if (BackupManifest.canCoverImage(ancestors, image)) {
          LOG.debug("Met the backup boundary of the current table set. "
              + "The root full backup images for the current backup scope:");
          for (BackupImage image1 : ancestors) {
            LOG.debug("  BackupId: " + image1.getBackupId() + ", Backup directory: "
                + image1.getRootDir());
          }
        } else {
          Path logBackupPath =
              HBackupFileSystem.getLogBackupPath(backup.getTargetRootDir(),
                backup.getBackupId());
          LOG.debug("Current backup has an incremental backup ancestor, "
              + "touching its image manifest in " + logBackupPath.toString()
              + " to construct the dependency.");
          BackupManifest lastIncrImgManifest = new BackupManifest(conf, logBackupPath);
          BackupImage lastIncrImage = lastIncrImgManifest.getBackupImage();
          ancestors.add(lastIncrImage);

          LOG.debug("Last dependent incremental backup image information:");
          LOG.debug("  Token: " + lastIncrImage.getBackupId());
          LOG.debug("  Backup directory: " + lastIncrImage.getRootDir());
        }
      }
    }
    LOG.debug("Got " + ancestors.size() + " ancestors for the current backup.");
    return ancestors;
  }

  /**
   * Get the direct ancestors of this backup for one table involved.
   * @param backupContext backup context
   * @param table table
   * @return backupImages on the dependency list
   * @throws BackupException exception
   * @throws IOException exception
   */
  public ArrayList<BackupImage> getAncestors(BackupInfo backupContext, TableName table)
      throws BackupException, IOException {
    ArrayList<BackupImage> ancestors = getAncestors(backupContext);
    ArrayList<BackupImage> tableAncestors = new ArrayList<BackupImage>();
    for (BackupImage image : ancestors) {
      if (image.hasTable(table)) {
        tableAncestors.add(image);
        if (image.getType() == BackupType.FULL) {
          break;
        }
      }
    }
    return tableAncestors;
  }

  /*
   * hbase:backup operations
   */

  /**
   * Updates status (state) of a backup session in a persistent store
   * @param context context
   * @throws IOException exception
   */
  public void updateBackupInfo(BackupInfo context) throws IOException {
    systemTable.updateBackupInfo(context);
  }

  /**
   * Read the last backup start code (timestamp) of last successful backup. Will return null
   * if there is no startcode stored in hbase:backup or the value is of length 0. These two
   * cases indicate there is no successful backup completed so far.
   * @return the timestamp of a last successful backup
   * @throws IOException exception
   */
  public String readBackupStartCode() throws IOException {
    return systemTable.readBackupStartCode(backupContext.getTargetRootDir());
  }

  /**
   * Write the start code (timestamp) to hbase:backup. If passed in null, then write 0 byte.
   * @param startCode start code
   * @throws IOException exception
   */
  public void writeBackupStartCode(Long startCode) throws IOException {
    systemTable.writeBackupStartCode(startCode, backupContext.getTargetRootDir());
  }

  /**
   * Get the RS log information after the last log roll from hbase:backup.
   * @return RS log info
   * @throws IOException exception
   */
  public HashMap<String, Long> readRegionServerLastLogRollResult() throws IOException {
    return systemTable.readRegionServerLastLogRollResult(backupContext.getTargetRootDir());
  }

  /**
   * Get all completed backup information (in desc order by time)
   * @return history info of BackupCompleteData
   * @throws IOException exception
   */
  public ArrayList<BackupInfo> getBackupHistory() throws IOException {
    return systemTable.getBackupHistory();
  }

  public ArrayList<BackupInfo> getBackupHistory(boolean completed) throws IOException {
    return systemTable.getBackupHistory(completed);
  }
  /**
   * Write the current timestamps for each regionserver to hbase:backup after a successful full or
   * incremental backup. Each table may have a different set of log timestamps. The saved timestamp
   * is of the last log file that was backed up already.
   * @param tables tables
   * @throws IOException exception
   */
  public void writeRegionServerLogTimestamp(Set<TableName> tables,
      HashMap<String, Long> newTimestamps) throws IOException {
    systemTable.writeRegionServerLogTimestamp(tables, newTimestamps, 
      backupContext.getTargetRootDir());
  }

  /**
   * Read the timestamp for each region server log after the last successful backup. Each table has
   * its own set of the timestamps.
   * @return the timestamp for each region server. key: tableName value:
   *         RegionServer,PreviousTimeStamp
   * @throws IOException exception
   */
  public HashMap<TableName, HashMap<String, Long>> readLogTimestampMap() throws IOException {
    return systemTable.readLogTimestampMap(backupContext.getTargetRootDir());
  }

  /**
   * Return the current tables covered by incremental backup.
   * @return set of tableNames
   * @throws IOException exception
   */
  public Set<TableName> getIncrementalBackupTableSet() throws IOException {
    return systemTable.getIncrementalBackupTableSet(backupContext.getTargetRootDir());
  }

  /**
   * Adds set of tables to overall incremental backup table set
   * @param tables tables
   * @throws IOException exception
   */
  public void addIncrementalBackupTableSet(Set<TableName> tables) throws IOException {
    systemTable.addIncrementalBackupTableSet(tables, backupContext.getTargetRootDir());
  }

  /**
   * Saves list of WAL files after incremental backup operation. These files will be stored until
   * TTL expiration and are used by Backup Log Cleaner plugin to determine which WAL files can be
   * safely purged.
   */
  public void recordWALFiles(List<String> files) throws IOException {
    systemTable.addWALFiles(files, 
      backupContext.getBackupId(), backupContext.getTargetRootDir());
  }

  /**
   * Get WAL files iterator
   * @return WAL files iterator from hbase:backup
   * @throws IOException
   */
  public Iterator<BackupSystemTable.WALItem> getWALFilesFromBackupSystem() throws IOException {
    return  systemTable.getWALFilesIterator(backupContext.getTargetRootDir());
  }

  public Connection getConnection() {
    return conn;
  }
}
