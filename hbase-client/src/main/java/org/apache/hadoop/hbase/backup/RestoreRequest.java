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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving

/**
 * POJO class for restore request
 */
public class RestoreRequest {
  
  private String backupRootDir;  
  private String backupId;  
  private boolean check = false;
  private TableName[] fromTables;
  private TableName[] toTables;
  private boolean overwrite = false;
  
  public RestoreRequest(){    
  }

  public String getBackupRootDir() {
    return backupRootDir;
  }

  public RestoreRequest setBackupRootDir(String backupRootDir) {
    this.backupRootDir = backupRootDir;
    return this;
  }

  public String getBackupId() {
    return backupId;
  }

  public RestoreRequest setBackupId(String backupId) {
    this.backupId = backupId;
    return this;
  }

  public boolean isCheck() {
    return check;
  }

  public RestoreRequest setCheck(boolean check) {
    this.check = check;
    return this;
  }

  public TableName[] getFromTables() {
    return fromTables;
  }

  public RestoreRequest setFromTables(TableName[] fromTables) {
    this.fromTables = fromTables;
    return this;

  }

  public TableName[] getToTables() {
    return toTables;
  }

  public RestoreRequest setToTables(TableName[] toTables) {
    this.toTables = toTables;
    return this;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public RestoreRequest setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    return this;
  }
          
  
}
