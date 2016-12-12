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
import org.apache.hadoop.hbase.backup.BackupAdmin;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.backup.util.RestoreServerUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.collect.Lists;

/**
 * The administrative API implementation for HBase Backup . Create an instance from
 * {@link #BackupAdminImpl(Connection)} and call {@link #close()} afterwards.
 * <p>BackupAdmin can be used to create backups, restore data from backups and for
 * other backup-related operations.
 *
 * @see Admin
 * @since 2.0
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class BackupAdminImpl implements BackupAdmin {
  private static final Log LOG = LogFactory.getLog(BackupAdminImpl.class);

  private final Connection conn;

  public BackupAdminImpl(Connection conn) {
    this.conn = conn;
  }

  @Override
  public void close() throws IOException {
    if (conn != null) {
      conn.close();
    }
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
      BackupClientUtil.cleanupBackupData(backupInfo, conn.getConfiguration());
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
  public List<BackupInfo> getHistory(int n, BackupInfo.Filter ... filters) throws IOException {
    if (filters.length == 0) return getHistory(n);
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<BackupInfo> history = table.getBackupHistory();
      List<BackupInfo> result = new ArrayList<BackupInfo>();
      for(BackupInfo bi: history) {
        if(result.size() == n) break;
        boolean passed = true;
        for(int i=0; i < filters.length; i++) {
          if(!filters[i].apply(bi)) {
            passed = false;
            break;
          }
        }
        if(passed) {
          result.add(bi);
        }
      }
      return result;
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
    try (final BackupSystemTable table = new BackupSystemTable(conn);
         final Admin admin = conn.getAdmin();) {
      for (int i = 0; i < tables.length; i++) {
        tableNames[i] = tables[i].getNameAsString();
        if (!admin.tableExists(TableName.valueOf(tableNames[i]))) {
          throw new IOException("Cannot add " + tableNames[i] + " because it doesn't exist");
        }
      }
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
    if (request.isCheck()) {
      HashMap<TableName, BackupManifest> backupManifestMap = new HashMap<>();
      // check and load backup image manifest for the tables
      Path rootPath = new Path(request.getBackupRootDir());
      String backupId = request.getBackupId();
      TableName[] sTableArray = request.getFromTables();
      HBackupFileSystem.checkImageManifestExist(backupManifestMap,
        sTableArray, conn.getConfiguration(), rootPath, backupId);

      // Check and validate the backup image and its dependencies

        if (RestoreServerUtil.validate(backupManifestMap, conn.getConfiguration())) {
          LOG.info("Checking backup images: ok");
        } else {
          String errMsg = "Some dependencies are missing for restore";
          LOG.error(errMsg);
          throw new IOException(errMsg);
        }

    }
    // Execute restore request
    new RestoreTablesClient(conn, request).execute();
  }

  @Override
  public Future<Void> restoreAsync(RestoreRequest request) throws IOException {
    throw new UnsupportedOperationException("Asynchronous restore is not supported yet");
  }

  @Override
  public String backupTables(final BackupRequest request) throws IOException {
    String setName = request.getBackupSetName();
    BackupType type = request.getBackupType();
    String targetRootDir = request.getTargetRootDir();
    List<TableName> tableList = request.getTableList();

    String backupId =
        (setName == null || setName.length() == 0 ? BackupRestoreConstants.BACKUPID_PREFIX
            : setName + "_") + EnvironmentEdgeManager.currentTime();
    if (type == BackupType.INCREMENTAL) {
      Set<TableName> incrTableSet = null;
      try (BackupSystemTable table = new BackupSystemTable(conn)) {
        incrTableSet = table.getIncrementalBackupTableSet(targetRootDir);
      }

      if (incrTableSet.isEmpty()) {
        System.err.println("Incremental backup table set contains no table.\n"
            + "Use 'backup create full' or 'backup stop' to \n "
            + "change the tables covered by incremental backup.");
        throw new IOException("No table covered by incremental backup.");
      }

      tableList.removeAll(incrTableSet);
      if (!tableList.isEmpty()) {
        String extraTables = StringUtils.join(tableList, ",");
        System.err.println("Some tables (" + extraTables + ") haven't gone through full backup");
        throw new IOException("Perform full backup on " + extraTables + " first, "
            + "then retry the command");
      }
      System.out.println("Incremental backup for the following table set: " + incrTableSet);
      tableList = Lists.newArrayList(incrTableSet);
    }
    if (tableList != null && !tableList.isEmpty()) {
      for (TableName table : tableList) {
        String targetTableBackupDir =
            HBackupFileSystem.getTableBackupDir(targetRootDir, backupId, table);
        Path targetTableBackupDirPath = new Path(targetTableBackupDir);
        FileSystem outputFs =
            FileSystem.get(targetTableBackupDirPath.toUri(), conn.getConfiguration());
        if (outputFs.exists(targetTableBackupDirPath)) {
          throw new IOException("Target backup directory " + targetTableBackupDir
              + " exists already.");
        }
      }
      ArrayList<TableName> nonExistingTableList = null;
      try (Admin admin = conn.getAdmin();) {
        for (TableName tableName : tableList) {
          if (!admin.tableExists(tableName)) {
            if (nonExistingTableList == null) {
              nonExistingTableList = new ArrayList<>();
            }
            nonExistingTableList.add(tableName);
          }
        }
      }
      if (nonExistingTableList != null) {
        if (type == BackupType.INCREMENTAL) {
          System.err.println("Incremental backup table set contains non-exising table: "
              + nonExistingTableList);
          // Update incremental backup set
          tableList = excludeNonExistingTables(tableList, nonExistingTableList);
        } else {
          // Throw exception only in full mode - we try to backup non-existing table
          throw new IOException("Non-existing tables found in the table list: "
              + nonExistingTableList);
        }
      }
    }

    // update table list
    request.setTableList(tableList);

    if (type == BackupType.FULL) {
      new FullTableBackupClient(conn, backupId, request).execute();
    } else {
      new IncrementalTableBackupClient(conn, backupId, request).execute();
    }
    return backupId;
  }


  private List<TableName> excludeNonExistingTables(List<TableName> tableList,
      List<TableName> nonExistingTableList) {

    for (TableName table : nonExistingTableList) {
      tableList.remove(table);
    }
    return tableList;
  }

  @Override
  public Future<String> backupTablesAsync(final BackupRequest userRequest) throws IOException {
    throw new UnsupportedOperationException("Asynchronous backup is not supported yet");
  }

}
