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
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreClientFactory;
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
        ArrayList<BackupInfo> recentSessions =
            table.getBackupContexts(BackupState.RUNNING);
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
    BackupInfo backupInfo = null;
    String backupId = null;
    int totalDeleted = 0;
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      for (int i = 0; i < backupIds.length; i++) {
        backupId = backupIds[i];
        LOG.info("Deleting backup for backupID=" + backupId + " ...");
        backupInfo = table.readBackupInfo(backupId);
        if (backupInfo != null) {
          BackupClientUtil.cleanupBackupData(backupInfo, admin.getConfiguration());
          table.deleteBackupInfo(backupInfo.getBackupId());
          LOG.info("Delete backup for backupID=" + backupId + " completed.");
          totalDeleted++;
        } else {
          LOG.warn("Delete backup failed: no information found for backupID=" + backupId);
        }
      }
    }
    return totalDeleted;
  }

  @Override
  public List<BackupInfo> getHistory(int n) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
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
  public List<BackupSet> listBackupSets() throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
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
  public BackupSet getBackupSet(String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> list = table.describeBackupSet(name);
      return new BackupSet(name, list);
    }  
  }

  @Override
  public boolean deleteBackupSet(String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      if(table.describeBackupSet(name) == null) {
        return false;
      }
      table.deleteBackupSet(name);
      return true;
    }  
  }

  @Override
  public void addToBackupSet(String name, TableName[] tables) throws IOException {
    String[] tableNames = new String[tables.length];
    for(int i = 0; i < tables.length; i++){
      tableNames[i] = tables[i].getNameAsString();
    }
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.addToBackupSet(name, tableNames);      
      LOG.info("Added tables ["+StringUtils.join(tableNames, " ")+"] to '" + name + "' backup set");
    }  
  }

  @Override
  public void removeFromBackupSet(String name, String[] tables) throws IOException {
    LOG.info("Removing tables ["+ StringUtils.join(tables, " ")+"] from '" + name + "'");
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {      
      table.removeFromBackupSet(name, tables);
      LOG.info("Removing tables ["+ StringUtils.join(tables, " ")+"] from '" + name + "' completed.");
    }   
  }

  @Override
  public void restore(RestoreRequest request) throws IOException {
    RestoreClient client = BackupRestoreClientFactory.getRestoreClient(admin.getConfiguration());
    client.restore(request.getBackupRootDir(), 
                   request.getBackupId(), 
                   request.isCheck(), 
                   request.isAutorestore(), 
                   request.getFromTables(), 
                   request.getToTables(), 
                   request.isOverwrite());
    
  }

  @Override
  public String backupTables(final BackupRequest userRequest) 
      throws IOException {
   return admin.backupTables(userRequest);
  }
  
  
  @Override
  public Future<String> backupTablesAsync(final BackupRequest userRequest) 
      throws IOException {
    return admin.backupTablesAsync(userRequest);
  }
  
  
}
