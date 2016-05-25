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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
/**
 * The administrative API for HBase Backup. Obtain an instance from 
 * an {@link Admin#getBackupAdmin()} and call {@link #close()} afterwards.
 * <p>BackupAdmin can be used to create backups, restore data from backups and for 
 * other backup-related operations. 
 *
 * @see Admin
 * @since 2.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving

public interface BackupAdmin extends Closeable{

  /**
   * Backs up given list of tables fully. Synchronous operation.
   * 
   * @param request BackupRequest instance which contains the following members:
   *  type whether the backup is full or incremental
   *  tableList list of tables to backup
   *  targetRootDir root directory for saving the backup
   *  workers number of parallel workers. -1 - system defined
   *  bandwidth bandwidth per worker in MB per second. -1 - unlimited
   * @return the backup Id
   */
  
  public String backupTables(final BackupRequest userRequest) throws IOException;
  
  /**
   * Backs up given list of tables fully. Asynchronous operation.
   * 
   * @param request BackupRequest instance which contains the following members:
   *  type whether the backup is full or incremental
   *  tableList list of tables to backup
   *  targetRootDir root dir for saving the backup
   *  workers number of paralle workers. -1 - system defined
   *  bandwidth bandwidth per worker in MB per sec. -1 - unlimited
   * @return the backup Id future
   */
  public Future<String> backupTablesAsync(final BackupRequest userRequest) throws IOException;

  /**
   * Describe backup image command
   * @param backupId - backup id
   * @return backup info
   * @throws IOException exception
   */
  public BackupInfo getBackupInfo(String backupId) throws IOException;

  /**
   * Show backup progress command
   * @param backupId - backup id (may be null)
   * @return backup progress (0-100%), -1 if no active sessions
   *  or session not found
   * @throws IOException exception
   */
  public int getProgress(String backupId) throws IOException;

  /**
   * Delete backup image command
   * @param backupIds - backup id
   * @return total number of deleted sessions
   * @throws IOException exception
   */
  public int deleteBackups(String[] backupIds) throws IOException;

  /**
   * Show backup history command
   * @param n - last n backup sessions
   * @return list of backup infos
   * @throws IOException exception
   */
  public List<BackupInfo> getHistory(int n) throws IOException;

  /**
   * Backup sets list command - list all backup sets. Backup set is 
   * a named group of tables. 
   * @return all registered backup sets
   * @throws IOException exception
   */
  public List<BackupSet> listBackupSets() throws IOException;

  /**
   * Backup set describe command. Shows list of tables in
   * this particular backup set.
   * @param name set name
   * @return backup set description or null
   * @throws IOException exception
   */
  public BackupSet getBackupSet(String name) throws IOException;

  /**
   * Delete backup set command
   * @param name - backup set name
   * @return true, if success, false - otherwise 
   * @throws IOException exception
   */
  public boolean deleteBackupSet(String name) throws IOException;

  /**
   * Add tables to backup set command
   * @param name - name of backup set.
   * @param tables - list of tables to be added to this set.
   * @throws IOException exception
   */
  public void addToBackupSet(String name, TableName[] tables) throws IOException;

  /**
   * Remove tables from backup set
   * @param name - name of backup set.
   * @param tables - list of tables to be removed from this set.
   * @throws IOException exception
   */
  public void removeFromBackupSet(String name, String[] tables) throws IOException;
    
  /**
   * Restore backup
   * @param request - restore request
   * @throws IOException exception
   */
  public void restore(RestoreRequest request) throws IOException;
  
}
