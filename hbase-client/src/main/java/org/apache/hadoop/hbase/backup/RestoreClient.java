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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface RestoreClient {

  public void setConf(Configuration conf);

  /**
   * Restore operation.
   * @param backupRootDir The root dir for backup image
   * @param backupId The backup id for image to be restored
   * @param check True if only do dependency check
   * @param autoRestore True if automatically restore following the dependency
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @param isOverwrite True then do restore overwrite if target table exists, otherwise fail the
   *          request if target table exists
   * @throws IOException if any failure during restore
   */
  public  void restore(
      String backupRootDir,
      String backupId, boolean check, boolean autoRestore, TableName[] sTableArray,
      TableName[] tTableArray, boolean isOverwrite) throws IOException;
}
