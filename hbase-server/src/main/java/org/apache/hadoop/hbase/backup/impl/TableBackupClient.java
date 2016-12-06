/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.backup.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.Private
public abstract class TableBackupClient {
  private static final Log LOG = LogFactory.getLog(TableBackupClient.class);

  protected Configuration conf;
  protected Connection conn;
  protected String backupId;
  protected List<TableName> tableList;
  protected HashMap<String, Long> newTimestamps = null;

  protected BackupManager backupManager;
  protected BackupInfo backupContext;

  public TableBackupClient(final Connection conn, final String backupId,
      BackupRequest request)
      throws IOException {
    if (request.getBackupType() == BackupType.FULL) {
      backupManager = new BackupManager(conn, conn.getConfiguration());
    } else {
      backupManager = new IncrementalBackupManager(conn, conn.getConfiguration());
    }
    this.backupId = backupId;
    this.tableList = request.getTableList();
    this.conn = conn;
    this.conf = conn.getConfiguration();
    backupContext =
        backupManager.createBackupContext(backupId, request.getBackupType(), tableList,
          request.getTargetRootDir(),
          request.getWorkers(), request.getBandwidth());
    if (tableList == null || tableList.isEmpty()) {
      this.tableList = new ArrayList<>(backupContext.getTables());
    }
  }

  /**
   * Begin the overall backup.
   * @param backupContext backup context
   * @throws IOException exception
   */
  protected void beginBackup(BackupManager backupManager, BackupInfo backupContext) throws IOException {
    backupManager.setBackupContext(backupContext);
    // set the start timestamp of the overall backup
    long startTs = EnvironmentEdgeManager.currentTime();
    backupContext.setStartTs(startTs);
    // set overall backup status: ongoing
    backupContext.setState(BackupState.RUNNING);
    LOG.info("Backup " + backupContext.getBackupId() + " started at " + startTs + ".");

    backupManager.updateBackupInfo(backupContext);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Backup session " + backupContext.getBackupId() + " has been started.");
    }
  }

  private String getMessage(Exception e) {
    String msg = e.getMessage();
    if (msg == null || msg.equals("")) {
      msg = e.getClass().getName();
    }
    return msg;
  }

  /**
   * Delete HBase snapshot for backup.
   * @param backupCtx backup context
   * @throws Exception exception
   */
  private void
      deleteSnapshot(final Connection conn, BackupInfo backupCtx, Configuration conf)
          throws IOException {
    LOG.debug("Trying to delete snapshot for full backup.");
    for (String snapshotName : backupCtx.getSnapshotNames()) {
      if (snapshotName == null) {
        continue;
      }
      LOG.debug("Trying to delete snapshot: " + snapshotName);

      try (Admin admin = conn.getAdmin();) {
        admin.deleteSnapshot(snapshotName);
      } catch (IOException ioe) {
        LOG.debug("when deleting snapshot " + snapshotName, ioe);
      }
      LOG.debug("Deleting the snapshot " + snapshotName + " for backup " + backupCtx.getBackupId()
          + " succeeded.");
    }
  }

  /**
   * Clean up directories with prefix "exportSnapshot-", which are generated when exporting
   * snapshots.
   * @throws IOException exception
   */
  private void cleanupExportSnapshotLog(Configuration conf) throws IOException {
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);
    Path stagingDir =
        new Path(conf.get(BackupRestoreConstants.CONF_STAGING_ROOT, fs.getWorkingDirectory()
            .toString()));
    FileStatus[] files = FSUtils.listStatus(fs, stagingDir);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      if (file.getPath().getName().startsWith("exportSnapshot-")) {
        LOG.debug("Delete log files of exporting snapshot: " + file.getPath().getName());
        if (FSUtils.delete(fs, file.getPath(), true) == false) {
          LOG.warn("Can not delete " + file.getPath());
        }
      }
    }
  }

  /**
   * Clean up the uncompleted data at target directory if the ongoing backup has already entered the
   * copy phase.
   */
   private void cleanupTargetDir(BackupInfo backupContext, Configuration conf) {
    try {
      // clean up the uncompleted data at target directory if the ongoing backup has already entered
      // the copy phase
      LOG.debug("Trying to cleanup up target dir. Current backup phase: "
          + backupContext.getPhase());
      if (backupContext.getPhase().equals(BackupPhase.SNAPSHOTCOPY)
          || backupContext.getPhase().equals(BackupPhase.INCREMENTAL_COPY)
          || backupContext.getPhase().equals(BackupPhase.STORE_MANIFEST)) {
        FileSystem outputFs =
            FileSystem.get(new Path(backupContext.getTargetRootDir()).toUri(), conf);

        // now treat one backup as a transaction, clean up data that has been partially copied at
        // table level
        for (TableName table : backupContext.getTables()) {
          Path targetDirPath =
              new Path(HBackupFileSystem.getTableBackupDir(backupContext.getTargetRootDir(),
                backupContext.getBackupId(), table));
          if (outputFs.delete(targetDirPath, true)) {
            LOG.info("Cleaning up uncompleted backup data at " + targetDirPath.toString()
                + " done.");
          } else {
            LOG.info("No data has been copied to " + targetDirPath.toString() + ".");
          }

          Path tableDir = targetDirPath.getParent();
          FileStatus[] backups = FSUtils.listStatus(outputFs, tableDir);
          if (backups == null || backups.length == 0) {
            outputFs.delete(tableDir, true);
            LOG.debug(tableDir.toString() + " is empty, remove it.");
          }
        }
      }

    } catch (IOException e1) {
      LOG.error("Cleaning up uncompleted backup data of " + backupContext.getBackupId() + " at "
          + backupContext.getTargetRootDir() + " failed due to " + e1.getMessage() + ".");
    }
  }

  /**
   * Fail the overall backup.
   * @param backupContext backup context
   * @param e exception
   * @throws Exception exception
   */
  protected void failBackup(Connection conn, BackupInfo backupContext, BackupManager backupManager,
      Exception e, String msg, BackupType type, Configuration conf) throws IOException {
    LOG.error(msg + getMessage(e), e);
    // If this is a cancel exception, then we've already cleaned.

    // set the failure timestamp of the overall backup
    backupContext.setEndTs(EnvironmentEdgeManager.currentTime());

    // set failure message
    backupContext.setFailedMsg(e.getMessage());

    // set overall backup status: failed
    backupContext.setState(BackupState.FAILED);

    // compose the backup failed data
    String backupFailedData =
        "BackupId=" + backupContext.getBackupId() + ",startts=" + backupContext.getStartTs()
            + ",failedts=" + backupContext.getEndTs() + ",failedphase=" + backupContext.getPhase()
            + ",failedmessage=" + backupContext.getFailedMsg();
    LOG.error(backupFailedData);

    backupManager.updateBackupInfo(backupContext);

    // if full backup, then delete HBase snapshots if there already are snapshots taken
    // and also clean up export snapshot log files if exist
    if (type == BackupType.FULL) {
      deleteSnapshot(conn, backupContext, conf);
      cleanupExportSnapshotLog(conf);
    }

    // clean up the uncompleted data at target directory if the ongoing backup has already entered
    // the copy phase
    // For incremental backup, DistCp logs will be cleaned with the targetDir.
    cleanupTargetDir(backupContext, conf);
    LOG.info("Backup " + backupContext.getBackupId() + " failed.");
  }


  /**
   * Add manifest for the current backup. The manifest is stored within the table backup directory.
   * @param backupContext The current backup context
   * @throws IOException exception
   * @throws BackupException exception
   */
  private void addManifest(BackupInfo backupContext, BackupManager backupManager,
      BackupType type, Configuration conf) throws IOException, BackupException {
    // set the overall backup phase : store manifest
    backupContext.setPhase(BackupPhase.STORE_MANIFEST);

    BackupManifest manifest;

    // Since we have each table's backup in its own directory structure,
    // we'll store its manifest with the table directory.
    for (TableName table : backupContext.getTables()) {
      manifest = new BackupManifest(backupContext, table);
      ArrayList<BackupImage> ancestors = backupManager.getAncestors(backupContext, table);
      for (BackupImage image : ancestors) {
        manifest.addDependentImage(image);
      }

      if (type == BackupType.INCREMENTAL) {
        // We'll store the log timestamps for this table only in its manifest.
        HashMap<TableName, HashMap<String, Long>> tableTimestampMap =
            new HashMap<TableName, HashMap<String, Long>>();
        tableTimestampMap.put(table, backupContext.getIncrTimestampMap().get(table));
        manifest.setIncrTimestampMap(tableTimestampMap);
        ArrayList<BackupImage> ancestorss = backupManager.getAncestors(backupContext);
        for (BackupImage image : ancestorss) {
          manifest.addDependentImage(image);
        }
      }
      manifest.store(conf);
    }

    // For incremental backup, we store a overall manifest in
    // <backup-root-dir>/WALs/<backup-id>
    // This is used when created the next incremental backup
    if (type == BackupType.INCREMENTAL) {
      manifest = new BackupManifest(backupContext);
      // set the table region server start and end timestamps for incremental backup
      manifest.setIncrTimestampMap(backupContext.getIncrTimestampMap());
      ArrayList<BackupImage> ancestors = backupManager.getAncestors(backupContext);
      for (BackupImage image : ancestors) {
        manifest.addDependentImage(image);
      }
      manifest.store(conf);
    }
  }

  /**
   * Get backup request meta data dir as string.
   * @param backupContext backup context
   * @return meta data dir
   */
  private String obtainBackupMetaDataStr(BackupInfo backupContext) {
    StringBuffer sb = new StringBuffer();
    sb.append("type=" + backupContext.getType() + ",tablelist=");
    for (TableName table : backupContext.getTables()) {
      sb.append(table + ";");
    }
    if (sb.lastIndexOf(";") > 0) {
      sb.delete(sb.lastIndexOf(";"), sb.lastIndexOf(";") + 1);
    }
    sb.append(",targetRootDir=" + backupContext.getTargetRootDir());

    return sb.toString();
  }

  /**
   * Clean up directories with prefix "_distcp_logs-", which are generated when DistCp copying
   * hlogs.
   * @throws IOException exception
   */
  private void cleanupDistCpLog(BackupInfo backupContext, Configuration conf)
      throws IOException {
    Path rootPath = new Path(backupContext.getHLogTargetDir()).getParent();
    FileSystem fs = FileSystem.get(rootPath.toUri(), conf);
    FileStatus[] files = FSUtils.listStatus(fs, rootPath);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      if (file.getPath().getName().startsWith("_distcp_logs")) {
        LOG.debug("Delete log files of DistCp: " + file.getPath().getName());
        FSUtils.delete(fs, file.getPath(), true);
      }
    }
  }

  /**
   * Complete the overall backup.
   * @param backupContext backup context
   * @throws Exception exception
   */
  protected void completeBackup(final Connection conn, BackupInfo backupContext,
      BackupManager backupManager, BackupType type, Configuration conf) throws IOException {
    // set the complete timestamp of the overall backup
    backupContext.setEndTs(EnvironmentEdgeManager.currentTime());
    // set overall backup status: complete
    backupContext.setState(BackupState.COMPLETE);
    backupContext.setProgress(100);
    // add and store the manifest for the backup
    addManifest(backupContext, backupManager, type, conf);

    // after major steps done and manifest persisted, do convert if needed for incremental backup
    /* in-fly convert code here, provided by future jira */
    LOG.debug("in-fly convert code here, provided by future jira");

    // compose the backup complete data
    String backupCompleteData =
        obtainBackupMetaDataStr(backupContext) + ",startts=" + backupContext.getStartTs()
            + ",completets=" + backupContext.getEndTs() + ",bytescopied="
            + backupContext.getTotalBytesCopied();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Backup " + backupContext.getBackupId() + " finished: " + backupCompleteData);
    }
    backupManager.updateBackupInfo(backupContext);

    // when full backup is done:
    // - delete HBase snapshot
    // - clean up directories with prefix "exportSnapshot-", which are generated when exporting
    // snapshots
    if (type == BackupType.FULL) {
      deleteSnapshot(conn, backupContext, conf);
      cleanupExportSnapshotLog(conf);
    } else if (type == BackupType.INCREMENTAL) {
      cleanupDistCpLog(backupContext, conf);
    }
    LOG.info("Backup " + backupContext.getBackupId() + " completed.");
  }

  /**
   * Backup request execution
   * @throws IOException
   */
  public abstract void execute() throws IOException;


}

