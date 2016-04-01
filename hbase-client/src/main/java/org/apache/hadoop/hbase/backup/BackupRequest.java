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

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * POJO class for backup request
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BackupRequest {
  private BackupType type;
  private List<TableName> tableList;
  private String targetRootDir;
  private int workers = -1;
  private long bandwidth = -1L;

  public BackupRequest() {
  }

  public BackupRequest setBackupType(BackupType type) {
    this.type = type;
    return this;
  }
  public BackupType getBackupType() {
    return this.type;
  }

  public BackupRequest setTableList(List<TableName> tableList) {
    this.tableList = tableList;
    return this;
  }
  public List<TableName> getTableList() {
    return this.tableList;
  }

  public BackupRequest setTargetRootDir(String targetRootDir) {
    this.targetRootDir = targetRootDir;
    return this;
  }
  public String getTargetRootDir() {
    return this.targetRootDir;
  }

  public BackupRequest setWorkers(int workers) {
    this.workers = workers;
    return this;
  }
  public int getWorkers() {
    return this.workers;
  }

  public BackupRequest setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
    return this;
  }
  public long getBandwidth() {
    return this.bandwidth;
  }
}
