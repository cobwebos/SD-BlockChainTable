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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupUtility;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

/**
 * After a full backup was created, the incremental backup will only store the changes made
 * after the last full or incremental backup.
 *
 * Creating the backup copies the logfiles in .logs and .oldlogs since the last backup timestamp.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IncrementalBackupManager {
  public static final Log LOG = LogFactory.getLog(IncrementalBackupManager.class);

  // parent manager
  private final BackupManager backupManager;
  private final Configuration conf;
  private final Connection conn;

  public IncrementalBackupManager(BackupManager bm) {
    this.backupManager = bm;
    this.conf = bm.getConf();
    this.conn = bm.getConnection();
  }

  /**
   * Obtain the list of logs that need to be copied out for this incremental backup. The list is set
   * in BackupContext.
   * @param backupContext backup context
   * @return The new HashMap of RS log timestamps after the log roll for this incremental backup.
   * @throws IOException exception
   */
  public HashMap<String, Long> getIncrBackupLogFileList(BackupContext backupContext)
      throws IOException {
    List<String> logList;
    HashMap<String, Long> newTimestamps;
    HashMap<String, Long> previousTimestampMins;

    String savedStartCode = backupManager.readBackupStartCode();

    // key: tableName
    // value: <RegionServer,PreviousTimeStamp>
    HashMap<TableName, HashMap<String, Long>> previousTimestampMap =
        backupManager.readLogTimestampMap();

    previousTimestampMins = BackupUtil.getRSLogTimestampMins(previousTimestampMap);

    if (LOG.isDebugEnabled()) {
      LOG.debug("StartCode " + savedStartCode + "for backupID " + backupContext.getBackupId());
    }
    // get all new log files from .logs and .oldlogs after last TS and before new timestamp
    if (savedStartCode == null ||
        previousTimestampMins == null ||
          previousTimestampMins.isEmpty()) {
      throw new IOException("Cannot read any previous back up timestamps from hbase:backup. "
          + "In order to create an incremental backup, at least one full backup is needed.");
    }

    try (Admin admin = conn.getAdmin()) {
      LOG.info("Execute roll log procedure for incremental backup ...");
      admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
        LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, new HashMap<String, String>());
    }

    newTimestamps = backupManager.readRegionServerLastLogRollResult();

    logList = getLogFilesForNewBackup(previousTimestampMins, newTimestamps, conf, savedStartCode);
    logList.addAll(getLogFilesFromBackupSystem(previousTimestampMins, newTimestamps));
    backupContext.setIncrBackupFileList(logList);

    return newTimestamps;
  }

  /**
   * For each region server: get all log files newer than the last timestamps but not newer than the
   * newest timestamps. FROM hbase:backup table
   * @param olderTimestamps - the timestamp for each region server of the last backup.
   * @param newestTimestamps - the timestamp for each region server that the backup should lead to.
   * @return list of log files which needs to be added to this backup
   * @throws IOException
   */
  private List<String> getLogFilesFromBackupSystem(HashMap<String, Long> olderTimestamps,
    HashMap<String, Long> newestTimestamps) throws IOException {
    List<String> logFiles = new ArrayList<String>();
    Iterator<String> it = backupManager.getWALFilesFromBackupSystem();

    while (it.hasNext()) {
      String walFileName = it.next();
      String server = BackupUtil.parseHostNameFromLogFile(new Path(walFileName));
      //String server = getServer(walFileName);
      Long tss = getTimestamp(walFileName);
      Long oldTss = olderTimestamps.get(server);
      if (oldTss == null){
        logFiles.add(walFileName);
        continue;
      }
      Long newTss = newestTimestamps.get(server);
      if (newTss == null) {
        newTss = Long.MAX_VALUE;
      }

      if (tss > oldTss && tss < newTss) {
        logFiles.add(walFileName);
      }
    }
    return logFiles;
  }

  private Long getTimestamp(String walFileName) {
    int index = walFileName.lastIndexOf(BackupUtil.LOGNAME_SEPARATOR);
    return Long.parseLong(walFileName.substring(index+1));
  }

  /**
   * For each region server: get all log files newer than the last timestamps but not newer than the
   * newest timestamps.
   * @param olderTimestamps the timestamp for each region server of the last backup.
   * @param newestTimestamps the timestamp for each region server that the backup should lead to.
   * @param conf the Hadoop and Hbase configuration
   * @param savedStartCode the startcode (timestamp) of last successful backup.
   * @return a list of log files to be backed up
   * @throws IOException exception
   */
  private List<String> getLogFilesForNewBackup(HashMap<String, Long> olderTimestamps,
    HashMap<String, Long> newestTimestamps, Configuration conf, String savedStartCode)
        throws IOException {
    LOG.debug("In getLogFilesForNewBackup()\n" + "olderTimestamps: " + olderTimestamps
      + "\n newestTimestamps: " + newestTimestamps);
    Path rootdir = FSUtils.getRootDir(conf);
    Path logDir = new Path(rootdir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    FileSystem fs = rootdir.getFileSystem(conf);
    NewestLogFilter pathFilter = new NewestLogFilter();

    List<String> resultLogFiles = new ArrayList<String>();
    List<String> newestLogs = new ArrayList<String>();

    /*
     * The old region servers and timestamps info we kept in hbase:backup may be out of sync if new
     * region server is added or existing one lost. We'll deal with it here when processing the
     * logs. If data in hbase:backup has more hosts, just ignore it. If the .logs directory includes
     * more hosts, the additional hosts will not have old timestamps to compare with. We'll just use
     * all the logs in that directory. We always write up-to-date region server and timestamp info
     * to hbase:backup at the end of successful backup.
     */

    FileStatus[] rss;
    Path p;
    String host;
    Long oldTimeStamp;
    String currentLogFile;
    Long currentLogTS;

    // Get the files in .logs.
    rss = fs.listStatus(logDir);
    for (FileStatus rs : rss) {
      p = rs.getPath();
      host = BackupUtil.parseHostNameFromLogFile(p);
      FileStatus[] logs;
      oldTimeStamp = olderTimestamps.get(host);
      // It is possible that there is no old timestamp in hbase:backup for this host if
      // this region server is newly added after our last backup.
      if (oldTimeStamp == null) {
        logs = fs.listStatus(p);
      } else {
        pathFilter.setLastBackupTS(oldTimeStamp);
        logs = fs.listStatus(p, pathFilter);
      }
      for (FileStatus log : logs) {
        LOG.debug("currentLogFile: " + log.getPath().toString());
        if (DefaultWALProvider.isMetaFile(log.getPath())) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Skip hbase:meta log file: " + log.getPath().getName());
          }
          continue;
        }
        currentLogFile = log.getPath().toString();
        resultLogFiles.add(currentLogFile);
        currentLogTS = BackupUtility.getCreationTime(log.getPath());
        // newestTimestamps is up-to-date with the current list of hosts
        // so newestTimestamps.get(host) will not be null.
        if (Long.valueOf(currentLogTS) > Long.valueOf(newestTimestamps.get(host))) {
          newestLogs.add(currentLogFile);
        }
      }
    }

    // Include the .oldlogs files too.
    FileStatus[] oldlogs = fs.listStatus(oldLogDir);
    for (FileStatus oldlog : oldlogs) {
      p = oldlog.getPath();
      currentLogFile = p.toString();
      if (DefaultWALProvider.isMetaFile(p)) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Skip .meta log file: " + currentLogFile);
        }
        continue;
      }
      host = BackupUtility.parseHostFromOldLog(p);
      currentLogTS = BackupUtility.getCreationTime(p);
      oldTimeStamp = olderTimestamps.get(host);
      /*
       * It is possible that there is no old timestamp in hbase:backup for this host. At the time of
       * our last backup operation, this rs did not exist. The reason can be one of the two: 1. The
       * rs already left/crashed. Its logs were moved to .oldlogs. 2. The rs was added after our
       * last backup.
       */
      if (oldTimeStamp == null) {
        if (Long.valueOf(currentLogTS) < Long.valueOf(savedStartCode)) {
          // This log file is really old, its region server was before our last backup.
          continue;
        } else {
          resultLogFiles.add(currentLogFile);
        }
      } else if (Long.valueOf(currentLogTS) > Long.valueOf(oldTimeStamp)) {
        resultLogFiles.add(currentLogFile);
      }

      // It is possible that a host in .oldlogs is an obsolete region server
      // so newestTimestamps.get(host) here can be null.
      // Even if these logs belong to a obsolete region server, we still need
      // to include they to avoid loss of edits for backup.
      Long newTimestamp = newestTimestamps.get(host);
      if (newTimestamp != null && Long.valueOf(currentLogTS) > Long.valueOf(newTimestamp)) {
        newestLogs.add(currentLogFile);
      }
    }
    // remove newest log per host because they are still in use
    resultLogFiles.removeAll(newestLogs);
    return resultLogFiles;
  }

  class NewestLogFilter implements PathFilter {
    private Long lastBackupTS = 0L;

    public NewestLogFilter() {
    }

    protected void setLastBackupTS(Long ts) {
      this.lastBackupTS = ts;
    }

    @Override
    public boolean accept(Path path) {
      // skip meta table log -- ts.meta file
      if (DefaultWALProvider.isMetaFile(path)) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Skip .meta log file: " + path.getName());
        }
        return false;
      }
      Long timestamp = null;
      try {
        timestamp = BackupUtility.getCreationTime(path);
        return timestamp > Long.valueOf(lastBackupTS);
      } catch (IOException e) {
        LOG.warn("Cannot read timestamp of log file " + path);
        return false;
      }
    }
  }

}
