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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupUtil;
import org.apache.hadoop.hbase.backup.impl.IncrementalRestoreService;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MapReduceRestoreService implements IncrementalRestoreService {
  public static final Log LOG = LogFactory.getLog(MapReduceRestoreService.class);

  private WALPlayer player;

  public MapReduceRestoreService() {
    this.player = new WALPlayer();
  }

  @Override
  public void run(Path[] logDirPaths, TableName[] tableNames, TableName[] newTableNames)
      throws IOException {

    // WALPlayer reads all files in arbitrary directory structure and creates a Map task for each
    // log file
    String logDirs = StringUtils.join(logDirPaths, ",");
    LOG.info("Restore incremental backup from directory " + logDirs + " from hbase tables "
        + BackupUtil.join(tableNames) + " to tables " + BackupUtil.join(newTableNames));

    for (int i = 0; i < tableNames.length; i++) {
      
      LOG.info("Restore "+ tableNames[i] + " into "+ newTableNames[i]);
      
      Path bulkOutputPath = getBulkOutputDir(newTableNames[i].getNameAsString());
      String[] playerArgs =
          { logDirs, tableNames[i].getNameAsString(), newTableNames[i].getNameAsString()};

      int result = 0;
      int loaderResult = 0;
      try {
        Configuration conf = getConf();
        conf.set(WALPlayer.BULK_OUTPUT_CONF_KEY, bulkOutputPath.toString());        
        player.setConf(getConf());        
        result = player.run(playerArgs);
        if (succeeded(result)) {
          // do bulk load
          LoadIncrementalHFiles loader = createLoader();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Restoring HFiles from directory " + bulkOutputPath);
          }
          String[] args = { bulkOutputPath.toString(), newTableNames[i].getNameAsString() };
          loaderResult = loader.run(args);
          if(failed(loaderResult)) {
            throw new IOException("Can not restore from backup directory " + logDirs
              + " (check Hadoop and HBase logs). Bulk loader return code =" + loaderResult);
          }
        } else {
            throw new IOException("Can not restore from backup directory " + logDirs
                + " (check Hadoop/MR and HBase logs). WALPlayer return code =" + result);
        }
        LOG.debug("Restore Job finished:" + result);
      } catch (Exception e) {
        throw new IOException("Can not restore from backup directory " + logDirs
            + " (check Hadoop and HBase logs) ", e);
      }

    }
  }


  private boolean failed(int result) {
    return result != 0;
  }
  
  private boolean succeeded(int result) {
    return result == 0;
  }

  private LoadIncrementalHFiles createLoader()
      throws IOException {
    // set configuration for restore:
    // LoadIncrementalHFile needs more time
    // <name>hbase.rpc.timeout</name> <value>600000</value>
    // calculates
    Integer milliSecInHour = 3600000;
    Configuration conf = new Configuration(getConf());
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, milliSecInHour);
    
    // By default, it is 32 and loader will fail if # of files in any region exceed this
    // limit. Bad for snapshot restore.
    conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, Integer.MAX_VALUE);
    LoadIncrementalHFiles loader = null;
    try {
      loader = new LoadIncrementalHFiles(conf);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return loader;
  }

  private Path getBulkOutputDir(String tableName) throws IOException
  {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String tmp = conf.get("hbase.tmp.dir");
    Path path =  new Path(tmp + Path.SEPARATOR + "bulk_output-"+tableName + "-"
        + EnvironmentEdgeManager.currentTime());
    fs.deleteOnExit(path);
    return path;
  }
  

  @Override
  public Configuration getConf() {
    return player.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    this.player.setConf(conf);
  }

}
