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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupClient;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Backup HBase tables locally or on a remote cluster Serve as client entry point for the following
 * features: - Full Backup provide local and remote back/restore for a list of tables - Incremental
 * backup to build on top of full backup as daily/weekly backup - Convert incremental backup WAL
 * files into hfiles - Merge several backup images into one(like merge weekly into monthly) - Add
 * and remove table to and from Backup image - Cancel a backup process - Full backup based on
 * existing snapshot - Describe information of a backup image
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BackupClientImpl implements BackupClient{
  private static final Log LOG = LogFactory.getLog(BackupClientImpl.class);
  private Configuration conf;

  public BackupClientImpl() {
  }
   
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  

  @Override
  public BackupInfo getBackupInfo(String backupId) throws IOException {
    BackupInfo backupInfo = null;
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      backupInfo = table.readBackupInfo(backupId);
      return backupInfo;
    } 
  }

  @Override  
  public int getProgress(String backupId) throws IOException {
    BackupInfo backupInfo = null;
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      if (backupId == null) {
        ArrayList<BackupInfo> recentSessions =
            table.getBackupContexts(BackupState.RUNNING);
        if (recentSessions.isEmpty()) {
          LOG.warn("No ongonig sessions found.");
          return -1;
        }
        // else show status for all ongoing sessions
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
    BackupInfo backupInfo = null;
    String backupId = null;
    int totalDeleted = 0;
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      for (int i = 0; i < backupIds.length; i++) {
        backupId = backupIds[i];
        backupInfo = table.readBackupInfo(backupId);
        if (backupInfo != null) {
          BackupUtil.cleanupBackupData(backupInfo, conf);
          table.deleteBackupInfo(backupInfo.getBackupId());
          System.out.println("Delete backup for backupID=" + backupId + " completed.");
          totalDeleted++;
        } else {
          System.out.println("Delete backup failed: no information found for backupID=" + backupId);
        }
      }
    }
    return totalDeleted;
  }

//TODO: Cancel backup?  
  
//  @Override
//  public void cancelBackup(String backupId) throws IOException {
//    // Kill distributed job if active
//    // Backup MUST not be in COMPLETE state
//    try (final BackupSystemTable table = new BackupSystemTable(conf)) {
//      BackupContext backupContext = table.readBackupStatus(backupId);
//      String errMessage = null;
//      if (backupContext != null && backupContext.getState() != BackupState.COMPLETE) {
//        BackupUtil.cleanupBackupData(backupContext, conf);
//        table.deleteBackupStatus(backupContext.getBackupId());
//        byte[] jobId = backupContext.getJobId();
//        if(jobId != null) {
//          BackupCopyService service = BackupRestoreFactory.getBackupCopyService(conf);
//          service.cancelCopyJob(jobId);
//        } else{
//          errMessage = "Distributed Job ID is null for backup "+backupId +
//              " in "+ backupContext.getState() + " state.";
//        }
//      } else if( backupContext == null){  
//        errMessage = "No information found for backupID=" + backupId;
//      } else {
//        errMessage = "Can not cancel "+ backupId + " in " + backupContext.getState()+" state";
//      }
//      
//      if( errMessage != null) {
//        throw new IOException(errMessage);
//      }
//    }
//    // then clean backup image
//    deleteBackups(new String[] { backupId });
//  }
  
  @Override
  public List<BackupInfo> getHistory(int n) throws IOException {
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<BackupInfo> history = table.getBackupHistory();
      if( history.size() <= n) return history;
      List<BackupInfo> list = new ArrayList<BackupInfo>();
      for(int i=0; i < n; i++){
        list.add(history.get(i));
      }
      return list;
    }
  }


  @Override
  public List<BackupSet> listBackupSets() throws IOException{
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<String> list = table.listBackupSets();
      List<BackupSet> bslist = new ArrayList<BackupSet>();
      for (String s : list) {
        List<TableName> tables = table.describeBackupSet(s);
        bslist.add( new BackupSet(s, tables));
      }
      return bslist;
    }
  }
  
  
  @Override
  public BackupSet getBackupSet(String name) throws IOException{
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> list = table.describeBackupSet(name);
      return new BackupSet(name, list);
    }
  }

  @Override
  public boolean deleteBackupSet(String name) throws IOException {
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      if(table.describeBackupSet(name) == null) {
        return false;
      }
      table.deleteBackupSet(name);
      return true;
    }
  }

  @Override
  public void addToBackupSet(String name, String[] tablesOrNamespaces) throws IOException {
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.addToBackupSet(name, tablesOrNamespaces);
      System.out.println("Added tables to '" + name + "'");
    }
  }
  
  @Override
  public void removeFromBackupSet(String name, String[] tablesOrNamepsaces) throws IOException {
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.removeFromBackupSet(name, tablesOrNamepsaces);
      System.out.println("Removed tables from '" + name + "'");
    } 
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
