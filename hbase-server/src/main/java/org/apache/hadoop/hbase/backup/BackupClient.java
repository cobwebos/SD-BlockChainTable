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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.backup.util.BackupSet;

public interface BackupClient extends Configurable{

  /**
   * Describe backup image command
   * @param backupId - backup id
   * @return backup info
   * @throws IOException
   */
  public BackupInfo getBackupInfo(String backupId) throws IOException;

  /**
   * Show backup progress command
   * @param backupId - backup id (may be null)
   * @return backup progress (0-100%), -1 if no active sessions
   *  or session not found
   * @throws IOException
   */
  public int getProgress(String backupId) throws IOException;

  /**
   * Delete backup image command
   * @param backupIds - backup id
   * @return total number of deleted sessions
   * @throws IOException
   */
  public int deleteBackups(String[] backupIds) throws IOException;

//  /**
//  TODO: Phase 3
//   * Cancel current active backup command
//   * @param backupId - backup id
//   * @throws IOException
//   */
//  public void cancelBackup(String backupId) throws IOException;

  /**
   * Show backup history command
   * @param n - last n backup sessions
   * @throws IOException
   */
  public List<BackupInfo> getHistory(int n) throws IOException;

  /**
   * Backup sets list command - list all backup sets. Backup set is 
   * a named group of tables. 
   * @throws IOException
   */
  public List<BackupSet> listBackupSets() throws IOException;

  /**
   * Backup set describe command. Shows list of tables in
   * this particular backup set.
   * @param name set name
   * @return backup set description or null
   * @throws IOException
   */
  public BackupSet getBackupSet(String name) throws IOException;

  /**
   * Delete backup set command
   * @param name - backup set name
   * @return true, if success, false - otherwise 
   * @throws IOException
   */
  public boolean deleteBackupSet(String name) throws IOException;

  /**
   * Add tables to backup set command
   * @param name - name of backup set.
   * @param tables - list of tables to be added to this set.
   * @throws IOException
   */
  public void addToBackupSet(String name, String[] tablesOrNamespaces) throws IOException;

  /**
   * Remove tables from backup set
   * @param name - name of backup set.
   * @param tables - list of tables to be removed from this set.
   * @throws IOException
   */
  public void removeFromBackupSet(String name, String[] tablesOrNamepsaces) throws IOException;

 }
