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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * HConstants holds a bunch of HBase Backup and Restore constants
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class BackupRestoreConstants {


  // delimiter in tablename list in restore command
  public static final String TABLENAME_DELIMITER_IN_COMMAND = ",";

  public static final String CONF_STAGING_ROOT = "snapshot.export.staging.root";

  public static final String BACKUPID_PREFIX = "backup_";

  public static enum BackupCommand {
    CREATE, CANCEL, DELETE, DESCRIBE, HISTORY, STATUS, CONVERT, MERGE, STOP, SHOW, HELP,
  }

  private BackupRestoreConstants() {
    // Can't be instantiated with this ctor.
  }
}
