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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupRestoreClientFactory {
  private static final Log LOG = LogFactory.getLog(BackupRestoreClientFactory.class);

  private BackupRestoreClientFactory(){
    throw new AssertionError("Instantiating utility class...");
  }
  
    
  /**
   * Gets restore client implementation
   * @param conf - configuration
   * @return backup client
   */
  public static RestoreClient getRestoreClient(Configuration conf) {
    try{
      Class<?> cls =
        conf.getClassByName("org.apache.hadoop.hbase.backup.impl.RestoreClientImpl");
         
      RestoreClient client = (RestoreClient) ReflectionUtils.newInstance(cls, conf);
      client.setConf(conf);
      return client;
    } catch(Exception e){
      LOG.error("Can not instantiate RestoreClient. Make sure you have hbase-server jar in CLASSPATH", e);
    }
    return null;
  }
}
