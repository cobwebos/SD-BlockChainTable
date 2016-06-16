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

package org.apache.hadoop.hbase.backup.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupException;
import org.apache.hadoop.hbase.backup.impl.BackupRestoreConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

/**
 * A collection for methods used by multiple classes to backup HBase tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupServerUtil {
  protected static final Log LOG = LogFactory.getLog(BackupServerUtil.class);
  public static final String LOGNAME_SEPARATOR = ".";

  private BackupServerUtil(){
    throw new AssertionError("Instantiating utility class...");
  }

  public static void waitForSnapshot(SnapshotDescription snapshot, long max,
      SnapshotManager snapshotMgr, Configuration conf) throws IOException {
    boolean done = false;
    long start = EnvironmentEdgeManager.currentTime();
    int numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    long maxPauseTime = max / numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for snapshot '" +
        ClientSnapshotDescriptionUtils.toString(snapshot) + "'' to complete. (max " +
        maxPauseTime + " ms per retry)");
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
          HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        long sleep = HBaseAdmin.getPauseTime(tries++, pause);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for snapshot completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of snapshot ...");
      done = snapshotMgr.isSnapshotDone(snapshot);
    }
    if (!done) {
      throw new SnapshotCreationException("Snapshot '" + snapshot.getName()
          + "' wasn't completed in expectedTime:" + max + " ms", snapshot);
    }
  }

  /**
   * Loop through the RS log timestamp map for the tables, for each RS, find the min timestamp
   * value for the RS among the tables.
   * @param rsLogTimestampMap timestamp map
   * @return the min timestamp of each RS
   */
  public static HashMap<String, Long> getRSLogTimestampMins(
    HashMap<TableName, HashMap<String, Long>> rsLogTimestampMap) {

    if (rsLogTimestampMap == null || rsLogTimestampMap.isEmpty()) {
      return null;
    }

    HashMap<String, Long> rsLogTimestampMins = new HashMap<String, Long>();
    HashMap<String, HashMap<TableName, Long>> rsLogTimestampMapByRS =
        new HashMap<String, HashMap<TableName, Long>>();

    for (Entry<TableName, HashMap<String, Long>> tableEntry : rsLogTimestampMap.entrySet()) {
      TableName table = tableEntry.getKey();
      HashMap<String, Long> rsLogTimestamp = tableEntry.getValue();
      for (Entry<String, Long> rsEntry : rsLogTimestamp.entrySet()) {
        String rs = rsEntry.getKey();
        Long ts = rsEntry.getValue();
        if (!rsLogTimestampMapByRS.containsKey(rs)) {
          rsLogTimestampMapByRS.put(rs, new HashMap<TableName, Long>());
          rsLogTimestampMapByRS.get(rs).put(table, ts);
        } else {
          rsLogTimestampMapByRS.get(rs).put(table, ts);
        }
      }
    }

    for (String rs : rsLogTimestampMapByRS.keySet()) {
      rsLogTimestampMins.put(rs, BackupClientUtil.getMinValue(rsLogTimestampMapByRS.get(rs)));
    }

    return rsLogTimestampMins;
  }

  /**
   * copy out Table RegionInfo into incremental backup image need to consider move this logic into
   * HBackupFileSystem
   * @param backupContext backup context
   * @param conf configuration
   * @throws IOException exception
   * @throws InterruptedException exception
   */
  public static void copyTableRegionInfo(BackupInfo backupContext, Configuration conf)
      throws IOException, InterruptedException {

    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    // for each table in the table set, copy out the table info and region 
    // info files in the correct directory structure
    try (Connection conn = ConnectionFactory.createConnection(conf); 
        Admin admin = conn.getAdmin()) {

      for (TableName table : backupContext.getTables()) {

        if(!admin.tableExists(table)) {
          LOG.warn("Table "+ table+" does not exists, skipping it.");
          continue;
        }
        LOG.debug("Attempting to copy table info for:" + table);
        TableDescriptor orig = FSTableDescriptors.getTableDescriptorFromFs(fs, rootDir, table);

        // write a copy of descriptor to the target directory
        Path target = new Path(backupContext.getBackupStatus(table).getTargetDir());
        FileSystem targetFs = target.getFileSystem(conf);
        FSTableDescriptors descriptors =
            new FSTableDescriptors(conf, targetFs, FSUtils.getRootDir(conf));
        descriptors.createTableDescriptorForTableDirectory(target, orig, false);
        LOG.debug("Finished copying tableinfo.");
        List<HRegionInfo> regions = null;
        regions = admin.getTableRegions(table);
        // For each region, write the region info to disk
        LOG.debug("Starting to write region info for table " + table);
        for (HRegionInfo regionInfo : regions) {
          Path regionDir =
              HRegion.getRegionDir(new Path(backupContext.getBackupStatus(table).getTargetDir()),
                regionInfo);
          regionDir =
              new Path(backupContext.getBackupStatus(table).getTargetDir(), regionDir.getName());
          writeRegioninfoOnFilesystem(conf, targetFs, regionDir, regionInfo);
        }
        LOG.debug("Finished writing region info for table " + table);
      }
    } catch (IOException e) {
      throw new BackupException(e);
    }
  }

  /**
   * Write the .regioninfo file on-disk.
   */
  public static void writeRegioninfoOnFilesystem(final Configuration conf, final FileSystem fs,
      final Path regionInfoDir, HRegionInfo regionInfo) throws IOException {
    final byte[] content = regionInfo.toDelimitedByteArray();
    Path regionInfoFile = new Path(regionInfoDir, ".regioninfo");
    // First check to get the permissions
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // Write the RegionInfo file content
    FSDataOutputStream out = FSUtils.create(conf, fs, regionInfoFile, perms, null);
    try {
      out.write(content);
    } finally {
      out.close();
    }
  }

  /**
   * TODO: return hostname:port
   * @param p
   * @return host name: port
   * @throws IOException
   */
  public static String parseHostNameFromLogFile(Path p) {
    try {
      if (isArchivedLogFile(p)) {
        return BackupClientUtil.parseHostFromOldLog(p);
      } else {
        ServerName sname = DefaultWALProvider.getServerNameFromWALDirectoryName(p);
        return sname.getHostname() + ":" + sname.getPort();
      }
    } catch (Exception e) {
      LOG.warn("Skip log file (can't parse): " + p);
      return null;
    }
  }

  private static boolean isArchivedLogFile(Path p) {
    String oldLog = Path.SEPARATOR + HConstants.HREGION_OLDLOGDIR_NAME + Path.SEPARATOR;
    return p.toString().contains(oldLog);
  }

  /**
   * Returns WAL file name
   * @param walFileName WAL file name
   * @return WAL file name
   * @throws IOException exception
   * @throws IllegalArgumentException exception
   */
  public static String getUniqueWALFileNamePart(String walFileName) throws IOException {
    return getUniqueWALFileNamePart(new Path(walFileName));
  }

  /**
   * Returns WAL file name
   * @param p - WAL file path
   * @return WAL file name
   * @throws IOException exception
   */
  public static String getUniqueWALFileNamePart(Path p) throws IOException {
    return p.getName();
  }

  /**
   * Get the total length of files under the given directory recursively.
   * @param fs The hadoop file system
   * @param dir The target directory
   * @return the total length of files
   * @throws IOException exception
   */
  public static long getFilesLength(FileSystem fs, Path dir) throws IOException {
    long totalLength = 0;
    FileStatus[] files = FSUtils.listStatus(fs, dir);
    if (files != null) {
      for (FileStatus fileStatus : files) {
        if (fileStatus.isDirectory()) {
          totalLength += getFilesLength(fs, fileStatus.getPath());
        } else {
          totalLength += fileStatus.getLen();
        }
      }
    }
    return totalLength;
  }


  
  /**
   * Sort history list by start time in descending order.
   * @param historyList history list
   * @return sorted list of BackupCompleteData
   */
  public static ArrayList<BackupInfo> sortHistoryListDesc(
    ArrayList<BackupInfo> historyList) {
    ArrayList<BackupInfo> list = new ArrayList<BackupInfo>();
    TreeMap<String, BackupInfo> map = new TreeMap<String, BackupInfo>();
    for (BackupInfo h : historyList) {
      map.put(Long.toString(h.getStartTs()), h);
    }
    Iterator<String> i = map.descendingKeySet().iterator();
    while (i.hasNext()) {
      list.add(map.get(i.next()));
    }
    return list;
  }

  /**
   * Get list of all WAL files (WALs and archive)
   * @param c - configuration
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getListOfWALFiles(Configuration c) throws IOException {
    Path rootDir = FSUtils.getRootDir(c);
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<String>();

    FileSystem fs = FileSystem.get(c);
    logFiles = BackupClientUtil.getFiles(fs, logDir, logFiles, null);
    logFiles = BackupClientUtil.getFiles(fs, oldLogDir, logFiles, null);
    return logFiles;
  }

  /**
   * Get list of all WAL files (WALs and archive)
   * @param c - configuration
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getListOfWALFiles(Configuration c, PathFilter filter)
      throws IOException {
    Path rootDir = FSUtils.getRootDir(c);
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<String>();

    FileSystem fs = FileSystem.get(c);
    logFiles = BackupClientUtil.getFiles(fs, logDir, logFiles, filter);
    logFiles = BackupClientUtil.getFiles(fs, oldLogDir, logFiles, filter);
    return logFiles;
  }

  /**
   * Get list of all old WAL files (WALs and archive)
   * @param c - configuration
   * @param hostTimestampMap - host timestamp map
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getWALFilesOlderThan(final Configuration c,
    final HashMap<String, Long> hostTimestampMap) throws IOException {
    Path rootDir = FSUtils.getRootDir(c);
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<String>();

    PathFilter filter = new PathFilter() {

      @Override
      public boolean accept(Path p) {
        try {
          if (DefaultWALProvider.isMetaFile(p)) {
            return false;
          }
          String host = parseHostNameFromLogFile(p);
          if(host == null) {
            return false;
          }
          Long oldTimestamp = hostTimestampMap.get(host);
          Long currentLogTS = BackupClientUtil.getCreationTime(p);
          return currentLogTS <= oldTimestamp;
        } catch (Exception e) {
          LOG.warn("Can not parse"+ p, e);
          return false;
        }
      }
    };
    FileSystem fs = FileSystem.get(c);
    logFiles = BackupClientUtil.getFiles(fs, logDir, logFiles, filter);
    logFiles = BackupClientUtil.getFiles(fs, oldLogDir, logFiles, filter);
    return logFiles;
  }

  public static String join(TableName[] names) {
    StringBuilder sb = new StringBuilder();
    String sep = BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND;
    for (TableName s : names) {
      sb.append(sep).append(s.getNameAsString());
    }
    return sb.toString();
  }

  public static TableName[] parseTableNames(String tables) {
    if (tables == null) {
      return null;
    }
    String[] tableArray = tables.split(BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);

    TableName[] ret = new TableName[tableArray.length];
    for (int i = 0; i < tableArray.length; i++) {
      ret[i] = TableName.valueOf(tableArray[i]);
    }
    return ret;
  }
  
  public static void cleanupBackupData(BackupInfo context, Configuration conf) 
      throws IOException 
  {
    cleanupHLogDir(context, conf);
    cleanupTargetDir(context, conf);
  }

  /**
   * Clean up directories which are generated when DistCp copying hlogs.
   * @throws IOException
   */
  private static void cleanupHLogDir(BackupInfo backupContext, Configuration conf)
      throws IOException {

    String logDir = backupContext.getHLogTargetDir();
    if (logDir == null) {
      LOG.warn("No log directory specified for " + backupContext.getBackupId());
      return;
    }

    Path rootPath = new Path(logDir).getParent();
    FileSystem fs = FileSystem.get(rootPath.toUri(), conf);
    FileStatus[] files = FSUtils.listStatus(fs, rootPath);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      LOG.debug("Delete log files: " + file.getPath().getName());
      if(!FSUtils.delete(fs, file.getPath(), true)) {
        LOG.warn("Could not delete files in "+ file.getPath());
      };
    }
  }

  /**
   * Clean up the data at target directory
   */
  private static void cleanupTargetDir(BackupInfo backupContext, Configuration conf) {
    try {
      // clean up the data at target directory
      LOG.debug("Trying to cleanup up target dir : " + backupContext.getBackupId());
      String targetDir = backupContext.getTargetRootDir();
      if (targetDir == null) {
        LOG.warn("No target directory specified for " + backupContext.getBackupId());
        return;
      }

      FileSystem outputFs =
          FileSystem.get(new Path(backupContext.getTargetRootDir()).toUri(), conf);

      for (TableName table : backupContext.getTables()) {
        Path targetDirPath =
            new Path(HBackupFileSystem.getTableBackupDir(backupContext.getTargetRootDir(),
              backupContext.getBackupId(), table));
        if (outputFs.delete(targetDirPath, true)) {
          LOG.info("Cleaning up backup data at " + targetDirPath.toString() + " done.");
        } else {
          LOG.info("No data has been found in " + targetDirPath.toString() + ".");
        }

        Path tableDir = targetDirPath.getParent();
        FileStatus[] backups = FSUtils.listStatus(outputFs, tableDir);
        if (backups == null || backups.length == 0) {
          if(outputFs.delete(tableDir, true)){
            LOG.debug(tableDir.toString() + " is empty, remove it.");
          } else {
            LOG.warn("Could not delete "+ tableDir);
          }
        }
      }

    } catch (IOException e1) {
      LOG.error("Cleaning up backup data of " + backupContext.getBackupId() + " at "
          + backupContext.getTargetRootDir() + " failed due to " + e1.getMessage() + ".");
    }
  }
  

}
