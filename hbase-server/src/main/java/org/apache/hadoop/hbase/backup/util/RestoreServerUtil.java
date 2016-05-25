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

package org.apache.hadoop.hbase.backup.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupRestoreServerFactory;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.IncrementalRestoreService;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A collection for methods used by multiple classes to restore HBase tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RestoreServerUtil {

  public static final Log LOG = LogFactory.getLog(RestoreServerUtil.class);

  private final String[] ignoreDirs = { "recovered.edits" };

  protected Configuration conf = null;

  protected Path backupRootPath;

  protected String backupId;

  protected FileSystem fs;
  private final String RESTORE_TMP_PATH = "/tmp";
  private final Path restoreTmpPath;

  // store table name and snapshot dir mapping
  private final HashMap<TableName, Path> snapshotMap = new HashMap<>();

  public RestoreServerUtil(Configuration conf, final Path backupRootPath, final String backupId)
      throws IOException {
    this.conf = conf;
    this.backupRootPath = backupRootPath;
    this.backupId = backupId;
    this.fs = backupRootPath.getFileSystem(conf);
    this.restoreTmpPath = new Path(conf.get("hbase.fs.tmp.dir") != null?
        conf.get("hbase.fs.tmp.dir"): RESTORE_TMP_PATH,
      "restore");
  }

  /**
   * return value represent path for:
   * ".../user/biadmin/backup1/default/t1_dn/backup_1396650096738/archive/data/default/t1_dn"
   * @param tabelName table name
   * @return path to table archive
   * @throws IOException exception
   */
  Path getTableArchivePath(TableName tableName)
      throws IOException {
    Path baseDir = new Path(HBackupFileSystem.getTableBackupPath(tableName, backupRootPath, 
      backupId), HConstants.HFILE_ARCHIVE_DIRECTORY);
    Path dataDir = new Path(baseDir, HConstants.BASE_NAMESPACE_DIR);
    Path archivePath = new Path(dataDir, tableName.getNamespaceAsString());
    Path tableArchivePath =
        new Path(archivePath, tableName.getQualifierAsString());
    if (!fs.exists(tableArchivePath) || !fs.getFileStatus(tableArchivePath).isDirectory()) {
      LOG.debug("Folder tableArchivePath: " + tableArchivePath.toString() + " does not exists");
      tableArchivePath = null; // empty table has no archive
    }
    return tableArchivePath;
  }

  /**
   * Gets region list
   * @param tableName table name
   * @return RegionList region list
   * @throws FileNotFoundException exception
   * @throws IOException exception
   */
  ArrayList<Path> getRegionList(TableName tableName)
      throws FileNotFoundException, IOException {
    Path tableArchivePath = this.getTableArchivePath(tableName);
    ArrayList<Path> regionDirList = new ArrayList<Path>();
    FileStatus[] children = fs.listStatus(tableArchivePath);
    for (FileStatus childStatus : children) {
      // here child refer to each region(Name)
      Path child = childStatus.getPath();
      regionDirList.add(child);
    }
    return regionDirList;
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
  public void incrementalRestoreTable(Path[] logDirs,
      TableName[] tableNames, TableName[] newTableNames) throws IOException {

    if (tableNames.length != newTableNames.length) {
      throw new IOException("Number of source tables and target tables does not match!");
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
          BackupRestoreServerFactory.getIncrementalRestoreService(conf);

      restoreService.run(logDirs, tableNames, newTableNames);
    }
  }

  public void fullRestoreTable(Path tableBackupPath, TableName tableName, TableName newTableName,
      boolean converted) throws IOException {
    restoreTableAndCreate(tableName, newTableName, tableBackupPath, converted);
  }

  /**
   * return value represent path for:
   * ".../user/biadmin/backup1/default/t1_dn/backup_1396650096738/.hbase-snapshot"
   * @param backupRootPath backup root path
   * @param tableName table name
   * @param backupId backup Id
   * @return path for snapshot
   */
  static Path getTableSnapshotPath(Path backupRootPath, TableName tableName,
      String backupId) {
    return new Path(HBackupFileSystem.getTableBackupPath(tableName, backupRootPath, backupId),
      HConstants.SNAPSHOT_DIR_NAME);
  }

  /**
   * return value represent path for:
   * "..../default/t1_dn/backup_1396650096738/.hbase-snapshot/snapshot_1396650097621_default_t1_dn"
   * this path contains .snapshotinfo, .tabledesc (0.96 and 0.98) this path contains .snapshotinfo,
   * .data.manifest (trunk)
   * @param tableName table name
   * @return path to table info
   * @throws FileNotFoundException exception
   * @throws IOException exception
   */
  Path getTableInfoPath(TableName tableName)
      throws FileNotFoundException, IOException {
    Path tableSnapShotPath = getTableSnapshotPath(backupRootPath, tableName, backupId);
    Path tableInfoPath = null;

    // can't build the path directly as the timestamp values are different
    FileStatus[] snapshots = fs.listStatus(tableSnapShotPath);
    for (FileStatus snapshot : snapshots) {
      tableInfoPath = snapshot.getPath();
      // SnapshotManifest.DATA_MANIFEST_NAME = "data.manifest";
      if (tableInfoPath.getName().endsWith("data.manifest")) {
        break;
      }
    }
    return tableInfoPath;
  }

  /**
   * @param tableName is the table backed up
   * @return {@link HTableDescriptor} saved in backup image of the table
   */
  HTableDescriptor getTableDesc(TableName tableName)
      throws FileNotFoundException, IOException {
    Path tableInfoPath = this.getTableInfoPath(tableName);
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, tableInfoPath);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, tableInfoPath, desc);
    HTableDescriptor tableDescriptor = manifest.getTableDescriptor();
    if (!tableDescriptor.getTableName().equals(tableName)) {
      LOG.error("couldn't find Table Desc for table: " + tableName + " under tableInfoPath: "
          + tableInfoPath.toString());
      LOG.error("tableDescriptor.getNameAsString() = " + tableDescriptor.getNameAsString());
      throw new FileNotFoundException("couldn't find Table Desc for table: " + tableName + 
        " under tableInfoPath: " + tableInfoPath.toString());
    }
    return tableDescriptor;
  }

  /**
   * Duplicate the backup image if it's on local cluster
   * @see HStore#bulkLoadHFile(String, long)
   * @see HRegionFileSystem#bulkLoadStoreFile(String familyName, Path srcPath, long seqNum)
   * @param tableArchivePath archive path
   * @return the new tableArchivePath
   * @throws IOException exception
   */
  Path checkLocalAndBackup(Path tableArchivePath) throws IOException {
    // Move the file if it's on local cluster
    boolean isCopyNeeded = false;

    FileSystem srcFs = tableArchivePath.getFileSystem(conf);
    FileSystem desFs = FileSystem.get(conf);
    if (tableArchivePath.getName().startsWith("/")) {
      isCopyNeeded = true;
    } else {
      // This should match what is done in @see HRegionFileSystem#bulkLoadStoreFile(String, Path,
      // long)
      if (srcFs.getUri().equals(desFs.getUri())) {
        LOG.debug("cluster hold the backup image: " + srcFs.getUri() + "; local cluster node: "
            + desFs.getUri());
        isCopyNeeded = true;
      }
    }
    if (isCopyNeeded) {
      LOG.debug("File " + tableArchivePath + " on local cluster, back it up before restore");
      if (desFs.exists(restoreTmpPath)) {
        try {
          desFs.delete(restoreTmpPath, true);
        } catch (IOException e) {
          LOG.debug("Failed to delete path: " + restoreTmpPath
            + ", need to check whether restore target DFS cluster is healthy");
        }
      }
      FileUtil.copy(srcFs, tableArchivePath, desFs, restoreTmpPath, false, conf);
      LOG.debug("Copied to temporary path on local cluster: " + restoreTmpPath);
      tableArchivePath = restoreTmpPath;
    }
    return tableArchivePath;
  }

  private void restoreTableAndCreate(TableName tableName, TableName newTableName,
      Path tableBackupPath, boolean converted) throws IOException {
    if (newTableName == null || newTableName.equals("")) {
      newTableName = tableName;
    }

    FileSystem fileSys = tableBackupPath.getFileSystem(this.conf);

    // get table descriptor first
    HTableDescriptor tableDescriptor = null;

    Path tableSnapshotPath = getTableSnapshotPath(backupRootPath, tableName, backupId);

    if (fileSys.exists(tableSnapshotPath)) {
      // snapshot path exist means the backup path is in HDFS
      // check whether snapshot dir already recorded for target table
      if (snapshotMap.get(tableName) != null) {
        SnapshotDescription desc =
            SnapshotDescriptionUtils.readSnapshotInfo(fileSys, tableSnapshotPath);
        SnapshotManifest manifest = SnapshotManifest.open(conf, fileSys, tableSnapshotPath, desc);
        tableDescriptor = manifest.getTableDescriptor();
      } else {
        tableDescriptor = getTableDesc(tableName);
        snapshotMap.put(tableName, getTableInfoPath(tableName));
      }
      if (tableDescriptor == null) {
        LOG.debug("Found no table descriptor in the snapshot dir, previous schema would be lost");
      }
    } else if (converted) {
      // first check if this is a converted backup image
      LOG.error("convert will be supported in a future jira");
    }

    Path tableArchivePath = getTableArchivePath(tableName);
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
        ArrayList<Path> regionPathList = getRegionList(tableName);

        // should only try to create the table with all region informations, so we could pre-split
        // the regions in fine grain
        checkAndCreateTable(tableBackupPath, tableName, newTableName, regionPathList,
          tableDescriptor);
        if (tableArchivePath != null) {
          // start real restore through bulkload
          // if the backup target is on local cluster, special action needed
          Path tempTableArchivePath = checkLocalAndBackup(tableArchivePath);
          if (tempTableArchivePath.equals(tableArchivePath)) {
            if(LOG.isDebugEnabled()) {
              LOG.debug("TableArchivePath for bulkload using existPath: " + tableArchivePath);
            }
          } else {
            regionPathList = getRegionList(tempTableArchivePath); // point to the tempDir
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
   * Gets region list
   * @param tableArchivePath table archive path
   * @return RegionList region list
   * @throws FileNotFoundException exception
   * @throws IOException exception
   */
  ArrayList<Path> getRegionList(Path tableArchivePath) throws FileNotFoundException,
  IOException {
    ArrayList<Path> regionDirList = new ArrayList<Path>();
    FileStatus[] children = fs.listStatus(tableArchivePath);
    for (FileStatus childStatus : children) {
      // here child refer to each region(Name)
      Path child = childStatus.getPath();
      regionDirList.add(child);
    }
    return regionDirList;
  }

  /**
   * Counts the number of files in all subdirectories of an HBase table, i.e. HFiles.
   * @param regionPath Path to an HBase table directory
   * @return the number of files all directories
   * @throws IOException exception
   */
  int getNumberOfFilesInDir(Path regionPath) throws IOException {
    int result = 0;

    if (!fs.exists(regionPath) || !fs.getFileStatus(regionPath).isDirectory()) {
      throw new IllegalStateException("Cannot restore hbase table because directory '"
          + regionPath.toString() + "' is not a directory.");
    }

    FileStatus[] tableDirContent = fs.listStatus(regionPath);
    for (FileStatus subDirStatus : tableDirContent) {
      FileStatus[] colFamilies = fs.listStatus(subDirStatus.getPath());
      for (FileStatus colFamilyStatus : colFamilies) {
        FileStatus[] colFamilyContent = fs.listStatus(colFamilyStatus.getPath());
        result += colFamilyContent.length;
      }
    }
    return result;
  }

  /**
   * Counts the number of files in all subdirectories of an HBase tables, i.e. HFiles. And finds the
   * maximum number of files in one HBase table.
   * @param tableArchivePath archive path
   * @return the maximum number of files found in 1 HBase table
   * @throws IOException exception
   */
  int getMaxNumberOfFilesInSubDir(Path tableArchivePath) throws IOException {
    int result = 1;
    ArrayList<Path> regionPathList = getRegionList(tableArchivePath);
    // tableArchivePath = this.getTableArchivePath(tableName);

    if (regionPathList == null || regionPathList.size() == 0) {
      throw new IllegalStateException("Cannot restore hbase table because directory '"
          + tableArchivePath + "' is not a directory.");
    }

    for (Path regionPath : regionPathList) {
      result = Math.max(result, getNumberOfFilesInDir(regionPath));
    }
    return result;
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
        multipleTables ? getMaxNumberOfFilesInSubDir(tableArchivePath) :
            getNumberOfFilesInDir(tableArchivePath);
    Integer calculatedMillis = numberOfFilesInDir * milliSecInMin; // 1 minute per file
    Integer resultMillis = Math.max(calculatedMillis, previousMillis);
    if (resultMillis > previousMillis) {
      LOG.info("Setting configuration for restore with LoadIncrementalHFile: "
          + "hbase.rpc.timeout to " + calculatedMillis / milliSecInMin
          + " minutes, to handle the number of files in backup " + tableArchivePath);
      this.conf.setInt("hbase.rpc.timeout", resultMillis);
    }

    // By default, it is 32 and loader will fail if # of files in any region exceed this
    // limit. Bad for snapshot restore.
    this.conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, Integer.MAX_VALUE);
    LoadIncrementalHFiles loader = null;
    try {
      loader = new LoadIncrementalHFiles(this.conf);
    } catch (Exception e1) {
      throw new IOException(e1);
    }
    return loader;
  }

  /**
   * Calculate region boundaries and add all the column families to the table descriptor
   * @param regionDirList region dir list
   * @return a set of keys to store the boundaries
   */
  byte[][] generateBoundaryKeys(ArrayList<Path> regionDirList)
      throws FileNotFoundException, IOException {
    TreeMap<byte[], Integer> map = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    // Build a set of keys to store the boundaries
    byte[][] keys = null;
    // calculate region boundaries and add all the column families to the table descriptor
    for (Path regionDir : regionDirList) {
      LOG.debug("Parsing region dir: " + regionDir);
      Path hfofDir = regionDir;

      if (!fs.exists(hfofDir)) {
        LOG.warn("HFileOutputFormat dir " + hfofDir + " not found");
      }

      FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
      if (familyDirStatuses == null) {
        throw new IOException("No families found in " + hfofDir);
      }

      for (FileStatus stat : familyDirStatuses) {
        if (!stat.isDirectory()) {
          LOG.warn("Skipping non-directory " + stat.getPath());
          continue;
        }
        boolean isIgnore = false;
        String pathName = stat.getPath().getName();
        for (String ignore : ignoreDirs) {
          if (pathName.contains(ignore)) {
            LOG.warn("Skipping non-family directory" + pathName);
            isIgnore = true;
            break;
          }
        }
        if (isIgnore) {
          continue;
        }
        Path familyDir = stat.getPath();
        LOG.debug("Parsing family dir [" + familyDir.toString() + " in region [" + regionDir + "]");
        // Skip _logs, etc
        if (familyDir.getName().startsWith("_") || familyDir.getName().startsWith(".")) {
          continue;
        }

        // start to parse hfile inside one family dir
        Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
        for (Path hfile : hfiles) {
          if (hfile.getName().startsWith("_") || hfile.getName().startsWith(".")
              || StoreFileInfo.isReference(hfile.getName())
              || HFileLink.isHFileLink(hfile.getName())) {
            continue;
          }
          HFile.Reader reader = HFile.createReader(fs, hfile, new CacheConfig(conf), conf);
          final byte[] first, last;
          try {
            reader.loadFileInfo();
            first = reader.getFirstRowKey();
            last = reader.getLastRowKey();
            LOG.debug("Trying to figure out region boundaries hfile=" + hfile + " first="
                + Bytes.toStringBinary(first) + " last=" + Bytes.toStringBinary(last));

            // To eventually infer start key-end key boundaries
            Integer value = map.containsKey(first) ? (Integer) map.get(first) : 0;
            map.put(first, value + 1);
            value = map.containsKey(last) ? (Integer) map.get(last) : 0;
            map.put(last, value - 1);
          } finally {
            reader.close();
          }
        }
      }
    }
    keys = LoadIncrementalHFiles.inferBoundaries(map);
    return keys;
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

        byte[][] keys = generateBoundaryKeys(regionDirList);

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
