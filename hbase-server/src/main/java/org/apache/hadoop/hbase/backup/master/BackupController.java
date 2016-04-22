/**
 *
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
package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.coprocessor.BaseMasterAndRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;

/**
 * The current implementation checks if the backup system table 
 * (hbase:backup) exists on HBasae Master startup and if it does not -
 * it creates it. We need to make sure that backup system table is  
 * created under HBase user with ADMIN privileges
 */
public class BackupController extends BaseMasterAndRegionObserver {
  private static final Log LOG = LogFactory.getLog(BackupController.class.getName());

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // Need to create the new system table for backups (if does not exist)
    MasterServices master = ctx.getEnvironment().getMasterServices();
    HTableDescriptor backupHTD = BackupSystemTable.getSystemTableDescriptor();
    try{
      master.createTable(backupHTD, null, HConstants.NO_NONCE, HConstants.NO_NONCE);
      LOG.info("Created "+ BackupSystemTable.getTableNameAsString()+" table");
    } catch(TableExistsException e) {
      LOG.info("Table "+ BackupSystemTable.getTableNameAsString() +" already exists");
    } 
  }
}
