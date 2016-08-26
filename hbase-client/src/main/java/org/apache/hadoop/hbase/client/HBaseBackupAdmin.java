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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreClientFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.RestoreClient;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The administrative API implementation for HBase Backup . Obtain an instance from 
 * an {@link Admin#getBackupAdmin()} and call {@link #close()} afterwards.
 * <p>BackupAdmin can be used to create backups, restore data from backups and for 
 * other backup-related operations. 
 *
 * @see Admin
 * @since 2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class HBaseBackupAdmin implements BackupAdmin {
  private static final Log LOG = LogFactory.getLog(HBaseBackupAdmin.class);

  private final HBaseAdmin admin;
  private final Connection conn;

  HBaseBackupAdmin(HBaseAdmin admin) {
    this.admin = admin;
    this.conn = admin.getConnection();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public BackupInfo getBackupInfo(String backupId) throws IOException {
    BackupInfo backupInfo = null;
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      backupInfo = table.readBackupInfo(backupId);
      return backupInfo;
    }
  }

  @Override
  public int getProgress(String backupId) throws IOException {
    BackupInfo backupInfo = null;
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      if (backupId == null) {
        ArrayList<BackupInfo> recentSessions = table.getBackupContexts(BackupState.RUNNING);
        if (recentSessions.isEmpty()) {
          LOG.warn("No ongoing sessions found.");
          return -1;
        }
        // else show status for ongoing session
        // must be one maximum
        return recentSessions.get(0).getProgress();
      } else {

        backupInfo = table.readBackupInfo(backupId);
        if (backupInfo != null) {
          return backupInfo.getProgress();
        } else {
          LOG.warn("No information found for backupID=" + backupId);
          return -1;
        }
      }
    }
  }

  @Override
  public int deleteBackups(String[] backupIds) throws IOException {
    // TODO: requires FT, failure will leave system
    // in non-consistent state
    // see HBASE-15227

    int totalDeleted = 0;
    Map<String, HashSet<TableName>> allTablesMap = new HashMap<String, HashSet<TableName>>();

    try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
      for (int i = 0; i < backupIds.length; i++) {
        BackupInfo info = sysTable.readBackupInfo(backupIds[i]);
        if (info != null) {
          String rootDir = info.getTargetRootDir();
          HashSet<TableName> allTables = allTablesMap.get(rootDir);
          if (allTables == null) {
            allTables = new HashSet<TableName>();
            allTablesMap.put(rootDir, allTables);
          }
          allTables.addAll(info.getTableNames());
          totalDeleted += deleteBackup(backupIds[i], sysTable);
        }
      }
      finalizeDelete(allTablesMap, sysTable);
    }
    return totalDeleted;
  }

  /**
   * Updates incremental backup set for every backupRoot
   * @param tablesMap - Map [backupRoot: Set<TableName>]
   * @param table - backup system table
   * @throws IOException
   */

  private void finalizeDelete(Map<String, HashSet<TableName>> tablesMap, BackupSystemTable table)
      throws IOException {
    for (String backupRoot : tablesMap.keySet()) {
      Set<TableName> incrTableSet = table.getIncrementalBackupTableSet(backupRoot);
      Map<TableName, ArrayList<BackupInfo>> tableMap = 
          table.getBackupHistoryForTableSet(incrTableSet, backupRoot);      
      for(Map.Entry<TableName, ArrayList<BackupInfo>> entry: tableMap.entrySet()) {
        if(entry.getValue() == null) {
          // No more backups for a table
          incrTableSet.remove(entry.getKey());
        }
      }      
      if (!incrTableSet.isEmpty()) {
        table.addIncrementalBackupTableSet(incrTableSet, backupRoot);
      } else { // empty
        table.deleteIncrementalBackupTableSet(backupRoot);
      }
    }
  }
  
  /**
   * Delete single backup and all related backups
   * Algorithm:
   * 
   * Backup type: FULL or INCREMENTAL
   * Is this last backup session for table T: YES or NO
   * For every table T from table list 'tables':
   * if(FULL, YES) deletes only physical data (PD)
   * if(FULL, NO), deletes PD, scans all newer backups and removes T from backupInfo, until
   * we either reach the most recent backup for T in the system or FULL backup which
   * includes T
   * if(INCREMENTAL, YES) deletes only physical data (PD)
   * if(INCREMENTAL, NO) deletes physical data and for table T scans all backup images
   * between last FULL backup, which is older than the backup being deleted and the next
   * FULL backup (if exists) or last one for a particular table T and removes T from list
   * of backup tables.
   * @param backupId - backup id
   * @param sysTable - backup system table
   * @return total - number of deleted backup images
   * @throws IOException
   */
  private int deleteBackup(String backupId, BackupSystemTable sysTable) throws IOException {

    BackupInfo backupInfo = sysTable.readBackupInfo(backupId);

    int totalDeleted = 0;
    if (backupInfo != null) {
      LOG.info("Deleting backup " + backupInfo.getBackupId() + " ...");
      BackupClientUtil.cleanupBackupData(backupInfo, admin.getConfiguration());
      // List of tables in this backup;
      List<TableName> tables = backupInfo.getTableNames();
      long startTime = backupInfo.getStartTs();
      for (TableName tn : tables) {
        boolean isLastBackupSession = isLastBackupSession(sysTable, tn, startTime);
        if (isLastBackupSession) {
          continue;
        }
        // else
        List<BackupInfo> affectedBackups = getAffectedBackupInfos(backupInfo, tn, sysTable);
        for (BackupInfo info : affectedBackups) {
          if (info.equals(backupInfo)) {
            continue;
          }
          removeTableFromBackupImage(info, tn, sysTable);
        }
      }
      LOG.debug("Delete backup info "+ backupInfo.getBackupId());  

      sysTable.deleteBackupInfo(backupInfo.getBackupId());
      LOG.info("Delete backup " + backupInfo.getBackupId() + " completed.");
      totalDeleted++;
    } else {
      LOG.warn("Delete backup failed: no information found for backupID=" + backupId);
    }
    return totalDeleted;
  }

  private void removeTableFromBackupImage(BackupInfo info, TableName tn, BackupSystemTable sysTable)
      throws IOException {
    List<TableName> tables = info.getTableNames();
    LOG.debug("Remove "+ tn +" from " + info.getBackupId() + " tables=" + 
      info.getTableListAsString());
    if (tables.contains(tn)) {
      tables.remove(tn);

      if (tables.isEmpty()) {
        LOG.debug("Delete backup info "+ info.getBackupId());  

        sysTable.deleteBackupInfo(info.getBackupId());
        BackupClientUtil.cleanupBackupData(info, conn.getConfiguration());
      } else {
        info.setTables(tables);
        sysTable.updateBackupInfo(info);
        // Now, clean up directory for table
        cleanupBackupDir(info, tn, conn.getConfiguration());
      }
    }
  }

  private List<BackupInfo> getAffectedBackupInfos(BackupInfo backupInfo, TableName tn,
      BackupSystemTable table) throws IOException {
    LOG.debug("GetAffectedBackupInfos for: " + backupInfo.getBackupId() + " table=" + tn);
    long ts = backupInfo.getStartTs();
    List<BackupInfo> list = new ArrayList<BackupInfo>();
    List<BackupInfo> history = table.getBackupHistory(backupInfo.getTargetRootDir());
    // Scan from most recent to backupInfo
    // break when backupInfo reached
    for (BackupInfo info : history) {
      if (info.getStartTs() == ts) {
        break;
      }
      List<TableName> tables = info.getTableNames();
      if (tables.contains(tn)) {
        BackupType bt = info.getType();
        if (bt == BackupType.FULL) {
          // Clear list if we encounter FULL backup
          list.clear();
        } else {
          LOG.debug("GetAffectedBackupInfos for: " + backupInfo.getBackupId() + " table=" + tn
              + " added " + info.getBackupId() + " tables=" + info.getTableListAsString());
          list.add(info);
        }
      }
    }
    return list;
  }

  
  
  /**
   * Clean up the data at target directory
   * @throws IOException 
   */
  private void cleanupBackupDir(BackupInfo backupInfo, TableName table, Configuration conf) 
      throws IOException {
    try {
      // clean up the data at target directory
      String targetDir = backupInfo.getTargetRootDir();
      if (targetDir == null) {
        LOG.warn("No target directory specified for " + backupInfo.getBackupId());
        return;
      }

      FileSystem outputFs = FileSystem.get(new Path(backupInfo.getTargetRootDir()).toUri(), conf);

      Path targetDirPath =
          new Path(BackupClientUtil.getTableBackupDir(backupInfo.getTargetRootDir(),
            backupInfo.getBackupId(), table));
      if (outputFs.delete(targetDirPath, true)) {
        LOG.info("Cleaning up backup data at " + targetDirPath.toString() + " done.");
      } else {
        LOG.info("No data has been found in " + targetDirPath.toString() + ".");
      }

    } catch (IOException e1) {
      LOG.error("Cleaning up backup data of " + backupInfo.getBackupId() + " for table " + table
          + "at " + backupInfo.getTargetRootDir() + " failed due to " + e1.getMessage() + ".");
      throw e1;
    }
  }

  private boolean isLastBackupSession(BackupSystemTable table, TableName tn, long startTime)
      throws IOException {
    List<BackupInfo> history = table.getBackupHistory();
    for (BackupInfo info : history) {
      List<TableName> tables = info.getTableNames();
      if (!tables.contains(tn)) {
        continue;
      }
      if (info.getStartTs() <= startTime) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  @Override
  public List<BackupInfo> getHistory(int n) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<BackupInfo> history = table.getBackupHistory();
      if (history.size() <= n) return history;
      List<BackupInfo> list = new ArrayList<BackupInfo>();
      for (int i = 0; i < n; i++) {
        list.add(history.get(i));
      }
      return list;
    }
  }

  @Override
  public List<BackupInfo> getHistory(int n, TableName name) throws IOException {
    if (name == null) return getHistory(n);
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<BackupInfo> history = table.getBackupHistoryForTable(name);
      n = Math.min(n, history.size());
      return history.subList(0, n);
    }
  }

  @Override
  public List<BackupSet> listBackupSets() throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<String> list = table.listBackupSets();
      List<BackupSet> bslist = new ArrayList<BackupSet>();
      for (String s : list) {
        List<TableName> tables = table.describeBackupSet(s);
        if (tables != null) {
          bslist.add(new BackupSet(s, tables));
        }
      }
      return bslist;
    }
  }

  @Override
  public BackupSet getBackupSet(String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> list = table.describeBackupSet(name);
      if (list == null) return null;
      return new BackupSet(name, list);
    }
  }

  @Override
  public boolean deleteBackupSet(String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      if (table.describeBackupSet(name) == null) {
        return false;
      }
      table.deleteBackupSet(name);
      return true;
    }
  }

  @Override
  public void addToBackupSet(String name, TableName[] tables) throws IOException {
    String[] tableNames = new String[tables.length];
    for (int i = 0; i < tables.length; i++) {
      tableNames[i] = tables[i].getNameAsString();
      if (!admin.tableExists(TableName.valueOf(tableNames[i]))) {
        throw new IOException("Cannot add " + tableNames[i] + " because it doesn't exist");
      }
    }
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.addToBackupSet(name, tableNames);
      LOG.info("Added tables [" + StringUtils.join(tableNames, " ") + "] to '" + name
          + "' backup set");
    }
  }

  @Override
  public void removeFromBackupSet(String name, String[] tables) throws IOException {
    LOG.info("Removing tables [" + StringUtils.join(tables, " ") + "] from '" + name + "'");
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.removeFromBackupSet(name, tables);
      LOG.info("Removing tables [" + StringUtils.join(tables, " ") + "] from '" + name
          + "' completed.");
    }
  }

  @Override
  public void restore(RestoreRequest request) throws IOException {
    RestoreClient client = BackupRestoreClientFactory.getRestoreClient(admin.getConfiguration());
    client.restore(request.getBackupRootDir(), request.getBackupId(), request.isCheck(),
      request.getFromTables(), request.getToTables(), request.isOverwrite());

  }

  @Override
  public String backupTables(final BackupRequest userRequest) throws IOException {
    return admin.backupTables(userRequest);
  }

  @Override
  public Future<String> backupTablesAsync(final BackupRequest userRequest) throws IOException {
    return admin.backupTablesAsync(userRequest);
  }

}
