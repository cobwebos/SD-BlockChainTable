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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;

/**
 * A collection for methods used by multiple classes to restore HBase tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RestoreUtil {

  public static final Log LOG = LogFactory.getLog(RestoreUtil.class);

  protected Configuration conf = null;

  protected HBackupFileSystem hBackupFS = null;

  protected Path backupRootPath;

  protected String backupId;

  // store table name and snapshot dir mapping
  private final HashMap<TableName, Path> snapshotMap = new HashMap<>();

  public RestoreUtil(Configuration conf, HBackupFileSystem hBackupFS) throws IOException {
    this.conf = conf;
    this.hBackupFS = hBackupFS;
    this.backupRootPath = hBackupFS.getBackupRootPath();
    this.backupId = hBackupFS.getBackupId();
  }

  /**
   * During incremental backup operation. Call WalPlayer to replay WAL in backup image Currently
   * tableNames and newTablesNames only contain single table, will be expanded to multiple tables in
   * the future
   * @param logDir : incremental backup folders, which contains WAL
   * @param tableNames : source tableNames(table names were backuped)
   * @param newTableNames : target tableNames(table names to be restored to)
   * @throws IOException exception
   */
  public void incrementalRestoreTable(String logDir,
      TableName[] tableNames, TableName[] newTableNames) throws IOException {

    if (tableNames.length != newTableNames.length) {
      throw new IOException("Number of source tables adn taget Tables does not match!");
    }

    // for incremental backup image, expect the table already created either by user or previous
    // full backup. Here, check that all new tables exists
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin()) {
      for (TableName tableName : newTableNames) {
        if (!admin.tableExists(tableName)) {
          admin.close();
          throw new IOException("HBase table " + tableName
            + " does not exist. Create the table first, e.g. by restoring a full backup.");
        }
      }
      IncrementalRestoreService restoreService =
          BackupRestoreFactory.getIncrementalRestoreService(conf);

      restoreService.run(logDir, tableNames, newTableNames);
    }
  }

  public void fullRestoreTable(Path tableBackupPath, TableName tableName, TableName newTableName,
      boolean converted) throws IOException {
    restoreTableAndCreate(tableName, newTableName, tableBackupPath, converted);
  }

  private void restoreTableAndCreate(TableName tableName, TableName newTableName,
      Path tableBackupPath, boolean converted) throws IOException {
    if (newTableName == null || newTableName.equals("")) {
      newTableName = tableName;
    }

    FileSystem fileSys = tableBackupPath.getFileSystem(this.conf);

    // get table descriptor first
    HTableDescriptor tableDescriptor = null;

    Path tableSnapshotPath = HBackupFileSystem.getTableSnapshotPath(backupRootPath, tableName,
      backupId);

    if (fileSys.exists(tableSnapshotPath)) {
      // snapshot path exist means the backup path is in HDFS
      // check whether snapshot dir already recorded for target table
      if (snapshotMap.get(tableName) != null) {
        SnapshotDescription desc =
            SnapshotDescriptionUtils.readSnapshotInfo(fileSys, tableSnapshotPath);
        SnapshotManifest manifest = SnapshotManifest.open(conf, fileSys, tableSnapshotPath, desc);
        tableDescriptor = manifest.getTableDescriptor();


      } else {
        tableDescriptor = hBackupFS.getTableDesc(tableName);
        snapshotMap.put(tableName, hBackupFS.getTableInfoPath(tableName));
      }
      if (tableDescriptor == null) {
        LOG.debug("Found no table descriptor in the snapshot dir, previous schema would be lost");
      }
    } else if (converted) {
      // first check if this is a converted backup image
      LOG.error("convert will be supported in a future jira");
    }

    Path tableArchivePath = hBackupFS.getTableArchivePath(tableName);
    if (tableArchivePath == null) {
      if (tableDescriptor != null) {
        // find table descriptor but no archive dir means the table is empty, create table and exit
        if(LOG.isDebugEnabled()) {
          LOG.debug("find table descriptor but no archive dir for table " + tableName
            + ", will only create table");
        }
        tableDescriptor.setName(newTableName);
        checkAndCreateTable(tableBackupPath, tableName, newTableName, null, tableDescriptor);
        return;
      } else {
        throw new IllegalStateException("Cannot restore hbase table because directory '"
            + " tableArchivePath is null.");
      }
    }

    if (tableDescriptor == null) {
      tableDescriptor = new HTableDescriptor(newTableName);
    } else {
      tableDescriptor.setName(newTableName);
    }

    if (!converted) {
      // record all region dirs:
      // load all files in dir
      try {
        ArrayList<Path> regionPathList = hBackupFS.getRegionList(tableName);

        // should only try to create the table with all region informations, so we could pre-split
        // the regions in fine grain
        checkAndCreateTable(tableBackupPath, tableName, newTableName, regionPathList,
          tableDescriptor);
        if (tableArchivePath != null) {
          // start real restore through bulkload
          // if the backup target is on local cluster, special action needed
          Path tempTableArchivePath = hBackupFS.checkLocalAndBackup(tableArchivePath);
          if (tempTableArchivePath.equals(tableArchivePath)) {
            if(LOG.isDebugEnabled()) {
              LOG.debug("TableArchivePath for bulkload using existPath: " + tableArchivePath);
            }
          } else {
            regionPathList = hBackupFS.getRegionList(tempTableArchivePath); // point to the tempDir
            if(LOG.isDebugEnabled()) {
              LOG.debug("TableArchivePath for bulkload using tempPath: " + tempTableArchivePath);
            }
          }

          LoadIncrementalHFiles loader = createLoader(tempTableArchivePath, false);
          for (Path regionPath : regionPathList) {
            String regionName = regionPath.toString();
            if(LOG.isDebugEnabled()) {
              LOG.debug("Restoring HFiles from directory " + regionName);
            }
            String[] args = { regionName, newTableName.getNameAsString()};
            loader.run(args);
          }
        }
        // we do not recovered edits
      } catch (Exception e) {
        throw new IllegalStateException("Cannot restore hbase table", e);
      }
    } else {
      LOG.debug("convert will be supported in a future jira");
    }
  }



  /**
   * Create a {@link LoadIncrementalHFiles} instance to be used to restore the HFiles of a full
   * backup.
   * @return the {@link LoadIncrementalHFiles} instance
   * @throws IOException exception
   */
  private LoadIncrementalHFiles createLoader(Path tableArchivePath, boolean multipleTables)
      throws IOException {
    // set configuration for restore:
    // LoadIncrementalHFile needs more time
    // <name>hbase.rpc.timeout</name> <value>600000</value>
    // calculates
    Integer milliSecInMin = 60000;
    Integer previousMillis = this.conf.getInt("hbase.rpc.timeout", 0);
    Integer numberOfFilesInDir =
        multipleTables ? hBackupFS.getMaxNumberOfFilesInSubDir(tableArchivePath) : hBackupFS
            .getNumberOfFilesInDir(tableArchivePath);
    Integer calculatedMillis = numberOfFilesInDir * milliSecInMin; // 1 minute per file
    Integer resultMillis = Math.max(calculatedMillis, previousMillis);
    if (resultMillis > previousMillis) {
      LOG.info("Setting configuration for restore with LoadIncrementalHFile: "
          + "hbase.rpc.timeout to " + calculatedMillis / milliSecInMin
          + " minutes, to handle the number of files in backup " + tableArchivePath);
      this.conf.setInt("hbase.rpc.timeout", resultMillis);
    }

    LoadIncrementalHFiles loader = null;
    try {
      loader = new LoadIncrementalHFiles(this.conf);
    } catch (Exception e1) {
      throw new IOException(e1);
    }
    return loader;
  }

  /**
   * Prepare the table for bulkload, most codes copied from
   * {@link LoadIncrementalHFiles#createTable(String, String)}
   * @param tableBackupPath path
   * @param tableName table name
   * @param targetTableName target table name
   * @param regionDirList region directory list
   * @param htd table descriptor
   * @throws IOException exception
   */
  private void checkAndCreateTable(Path tableBackupPath, TableName tableName,
      TableName targetTableName, ArrayList<Path> regionDirList, HTableDescriptor htd)
          throws IOException {
    HBaseAdmin hbadmin = null;
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      hbadmin = (HBaseAdmin) conn.getAdmin();
      if (hbadmin.tableExists(targetTableName)) {
        LOG.info("Using exising target table '" + targetTableName + "'");
      } else {
        LOG.info("Creating target table '" + targetTableName + "'");

        // if no region dir given, create the table and return
        if (regionDirList == null || regionDirList.size() == 0) {

          hbadmin.createTable(htd);
          return;
        }

        byte[][] keys = hBackupFS.generateBoundaryKeys(regionDirList);

        // create table using table decriptor and region boundaries
        hbadmin.createTable(htd, keys);
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (hbadmin != null) {
        hbadmin.close();
      }
      if(conn != null){
        conn.close();
      }
    }
  }

}
