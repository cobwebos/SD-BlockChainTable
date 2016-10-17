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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupCopyTask;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceRestoreTask;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupRestoreServerFactory {

  public final static String HBASE_INCR_RESTORE_IMPL_CLASS = "hbase.incremental.restore.class";
  public final static String HBASE_BACKUP_COPY_IMPL_CLASS = "hbase.backup.copy.class";

  private BackupRestoreServerFactory(){
    throw new AssertionError("Instantiating utility class...");
  }
  
  /**
   * Gets backup restore service
   * @param conf - configuration
   * @return backup restore service instance
   */
  public static RestoreTask getRestoreService(Configuration conf) {
    Class<? extends RestoreTask> cls =
        conf.getClass(HBASE_INCR_RESTORE_IMPL_CLASS, MapReduceRestoreTask.class,
          RestoreTask.class);
    RestoreTask service =  ReflectionUtils.newInstance(cls, conf);
    service.setConf(conf);
    return service;
  }
  
  /**
   * Gets backup copy service
   * @param conf - configuration
   * @return backup copy service
   */
  public static BackupCopyTask getBackupCopyService(Configuration conf) {
    Class<? extends BackupCopyTask> cls =
        conf.getClass(HBASE_BACKUP_COPY_IMPL_CLASS, MapReduceBackupCopyTask.class,
          BackupCopyTask.class);
    BackupCopyTask service = ReflectionUtils.newInstance(cls, conf);;
    service.setConf(conf);
    return service;
  }
}
