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
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupClient;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.BackupClientUtil;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.collect.Lists;

/**
 * Backup HBase tables locally or on a remote cluster Serve as client entry point for the following
 * features: - Full Backup provide local and remote back/restore for a list of tables - Incremental
 * backup to build on top of full backup as daily/weekly backup - Convert incremental backup WAL
 * files into hfiles - Merge several backup images into one(like merge weekly into monthly) - Add
 * and remove table to and from Backup image - Cancel a backup process - Describe information of
 * a backup image
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BackupClientImpl implements BackupClient {
  private static final Log LOG = LogFactory.getLog(BackupClientImpl.class);
  private Configuration conf;
  private BackupManager backupManager;

  public BackupClientImpl() {
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Prepare and submit Backup request
   * @param backupId : backup_timestame (something like backup_1398729212626)
   * @param backupType : full or incremental
   * @param tableList : tables to be backuped
   * @param targetRootDir : specified by user
   * @throws IOException exception
   */
  protected void requestBackup(String backupId, BackupType backupType, List<TableName> tableList,
      String targetRootDir) throws IOException {

    BackupContext backupContext = null;

    HBaseAdmin hbadmin = null;
    Connection conn = null;
    try {
      backupManager = new BackupManager(conf);
      if (backupType == BackupType.INCREMENTAL) {
        Set<TableName> incrTableSet = backupManager.getIncrementalBackupTableSet();
        if (incrTableSet.isEmpty()) {
          LOG.warn("Incremental backup table set contains no table.\n"
              + "Use 'backup create full' or 'backup stop' to \n "
              + "change the tables covered by incremental backup.");
          throw new DoNotRetryIOException("No table covered by incremental backup.");
        }

        LOG.info("Incremental backup for the following table set: " + incrTableSet);
        tableList = Lists.newArrayList(incrTableSet);
      }

      // check whether table exists first before starting real request
      if (tableList != null) {
        ArrayList<TableName> nonExistingTableList = null;
        conn = ConnectionFactory.createConnection(conf);
        hbadmin = (HBaseAdmin) conn.getAdmin();
        for (TableName tableName : tableList) {
          if (!hbadmin.tableExists(tableName)) {
            if (nonExistingTableList == null) {
              nonExistingTableList = new ArrayList<>();
            }
            nonExistingTableList.add(tableName);
          }
        }
        if (nonExistingTableList != null) {
          if (backupType == BackupType.INCREMENTAL ) {
            LOG.warn("Incremental backup table set contains non-exising table: "
                + nonExistingTableList);
          } else {
            // Throw exception only in full mode - we try to backup non-existing table
            throw new DoNotRetryIOException("Non-existing tables found in the table list: "
                + nonExistingTableList);
          }
        }
      }

      // if any target table backup dir already exist, then no backup action taken
      if (tableList != null) {
        for (TableName table : tableList) {
          String targetTableBackupDir =
              HBackupFileSystem.getTableBackupDir(targetRootDir, backupId, table);
          Path targetTableBackupDirPath = new Path(targetTableBackupDir);
          FileSystem outputFs = FileSystem.get(targetTableBackupDirPath.toUri(), conf);
          if (outputFs.exists(targetTableBackupDirPath)) {
            throw new DoNotRetryIOException("Target backup directory " + targetTableBackupDir
              + " exists already.");
          }
        }
      }
      backupContext =
          backupManager.createBackupContext(backupId, backupType, tableList, targetRootDir);
      backupManager.initialize();
      backupManager.dispatchRequest(backupContext);
    } catch (BackupException e) {
      // suppress the backup exception wrapped within #initialize or #dispatchRequest, backup
      // exception has already been handled normally
      LOG.error("Backup Exception ", e);
    } finally {
      if (hbadmin != null) {
        hbadmin.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }

  @Override
  public String create(BackupType backupType, List<TableName> tableList, String backupRootPath)
      throws IOException {

    String backupId = BackupRestoreConstants.BACKUPID_PREFIX + EnvironmentEdgeManager.currentTime();
    BackupClientUtil.checkTargetDir(backupRootPath, conf);

    // table list specified for backup, trigger backup on specified tables
    try {
      requestBackup(backupId, backupType, tableList, backupRootPath);
    } catch (RuntimeException e) {
      String errMsg = e.getMessage();
      if (errMsg != null
          && (errMsg.startsWith("Non-existing tables found") || errMsg
              .startsWith("Snapshot is not found"))) {
        LOG.error(errMsg + ", please check your command");
        throw e;
      } else {
        throw e;
      }
    } finally{
      if(backupManager != null) {
        backupManager.close();
      }
    }
    return backupId;
  }

}
