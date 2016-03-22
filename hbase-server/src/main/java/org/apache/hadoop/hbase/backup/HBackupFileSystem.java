/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

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
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
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
 * View to an on-disk Backup Image FileSytem
 * Provides the set of methods necessary to interact with the on-disk Backup Image data.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HBackupFileSystem {
  public static final Log LOG = LogFactory.getLog(HBackupFileSystem.class);

  private final String RESTORE_TMP_PATH = "/tmp";
  private final String[] ignoreDirs = { "recovered.edits" };

  private final Configuration conf;
  private final FileSystem fs;
  private final Path backupRootPath;
  private final Path restoreTmpPath;
  private final String backupId;

  /**
   * Create a view to the on-disk Backup Image.
   * @param conf  to use
   * @param backupPath  to where the backup Image stored
   * @param backupId represent backup Image
   */
  public HBackupFileSystem(final Configuration conf, final Path backupRootPath, final String backupId)
      throws IOException {
    this.conf = conf;
    this.fs = backupRootPath.getFileSystem(conf);
    this.backupRootPath = backupRootPath;
    this.backupId = backupId; // the backup ID for the lead backup Image
    this.restoreTmpPath = new Path(conf.get("hbase.fs.tmp.dir") != null?
          conf.get("hbase.fs.tmp.dir"): RESTORE_TMP_PATH,
        "restore");
  }

  public Path getBackupRootPath() {
    return backupRootPath;
  }

  public String getBackupId() {
    return backupId;
  }

  /**
   * @param tableName is the table backed up
   * @return {@link HTableDescriptor} saved in backup image of the table
   */
  public HTableDescriptor getTableDesc(TableName tableName)
      throws FileNotFoundException, IOException {
    Path tableInfoPath = this.getTableInfoPath(tableName);
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, tableInfoPath);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, tableInfoPath, desc);
    HTableDescriptor tableDescriptor = manifest.getTableDescriptor();
    if (!tableDescriptor.getNameAsString().equals(tableName)) {
      LOG.error("couldn't find Table Desc for table: " + tableName + " under tableInfoPath: "
          + tableInfoPath.toString());
      LOG.error("tableDescriptor.getNameAsString() = " + tableDescriptor.getNameAsString());
    }
    return tableDescriptor;
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup1/default/t1_dn/backup_1396650096738"
   * @param backupRootDir backup root directory
   * @param backupId  backup id
   * @param table table name
   * @return backupPath String for the particular table
   */
  public static String getTableBackupDir(String backupRootDir, String backupId,
      TableName tableName) {
    return backupRootDir + Path.SEPARATOR + tableName.getNamespaceAsString() + Path.SEPARATOR
        + tableName.getQualifierAsString() + Path.SEPARATOR + backupId;
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup1/default/t1_dn/backup_1396650096738"
   * @param backupRootPath backup root path
   * @param tableName table name
   * @param backupId backup Id
   * @return backupPath for the particular table
   */
  public static Path getTableBackupPath(Path backupRootPath, TableName tableName, String backupId) {
    return new Path(backupRootPath, tableName.getNamespaceAsString() + Path.SEPARATOR
      + tableName.getQualifierAsString() + Path.SEPARATOR + backupId);
  }

  /**
   * return value represent path for:
   * ".../user/biadmin/backup1/default/t1_dn/backup_1396650096738/.hbase-snapshot"
   * @param backupRootPath backup root path
   * @param tableName table name
   * @param backupId backup Id
   * @return path for snapshot
   */
  public static Path getTableSnapshotPath(Path backupRootPath, TableName tableName,
      String backupId) {
    return new Path(getTableBackupPath(backupRootPath, tableName, backupId),
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
  public Path getTableInfoPath(TableName tableName)
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
   * return value represent path for:
   * ".../user/biadmin/backup1/default/t1_dn/backup_1396650096738/archive/data/default/t1_dn"
   * @param tabelName table name
   * @return path to table archive
   * @throws IOException exception
   */
  public Path getTableArchivePath(TableName tableName)
      throws IOException {
    Path baseDir = new Path(getTableBackupPath(backupRootPath, tableName, backupId),
      HConstants.HFILE_ARCHIVE_DIRECTORY);
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
   * Given the backup root dir and the backup id, return the log file location for an incremental
   * backup.
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @return logBackupDir: ".../user/biadmin/backup1/WALs/backup_1396650096738"
   */
  public static String getLogBackupDir(String backupRootDir, String backupId) {
    return backupRootDir + Path.SEPARATOR + HConstants.HREGION_LOGDIR_NAME + Path.SEPARATOR
        + backupId;
  }

  public static Path getLogBackupPath(String backupRootDir, String backupId) {
    return new Path(getLogBackupDir(backupRootDir, backupId));
  }

  private static Path getManifestPath(TableName tableName, Configuration conf,
      Path backupRootPath, String backupId) throws IOException {
    Path manifestPath = new Path(getTableBackupPath(backupRootPath, tableName, backupId),
      BackupManifest.MANIFEST_FILE_NAME);
    FileSystem fs = backupRootPath.getFileSystem(conf);
    if (!fs.exists(manifestPath)) {
      // check log dir for incremental backup case
      manifestPath =
          new Path(getLogBackupDir(backupRootPath.toString(), backupId) + Path.SEPARATOR
            + BackupManifest.MANIFEST_FILE_NAME);
      if (!fs.exists(manifestPath)) {
        String errorMsg =
            "Could not find backup manifest for " + backupId + " in " + backupRootPath.toString();
        throw new IOException(errorMsg);
      }
    }
    return manifestPath;
  }

  public static BackupManifest getManifest(TableName tableName, Configuration conf,
      Path backupRootPath, String backupId) throws IOException {
    BackupManifest manifest = new BackupManifest(conf,
      getManifestPath(tableName, conf, backupRootPath, backupId));
    return manifest;
  }

  /**
   * Gets region list
   * @param tableName table name
   * @return RegionList region list
   * @throws FileNotFoundException exception
   * @throws IOException exception
   */

  public ArrayList<Path> getRegionList(TableName tableName)
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
   * Gets region list
   * @param tableArchivePath table archive path
   * @return RegionList region list
   * @throws FileNotFoundException exception
   * @throws IOException exception
   */
  public ArrayList<Path> getRegionList(Path tableArchivePath) throws FileNotFoundException,
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
   * Counts the number of files in all subdirectories of an HBase tables, i.e. HFiles. And finds the
   * maximum number of files in one HBase table.
   * @param tableArchivePath archive path
   * @return the maximum number of files found in 1 HBase table
   * @throws IOException exception
   */
  public int getMaxNumberOfFilesInSubDir(Path tableArchivePath) throws IOException {
    int result = 1;
    ArrayList<Path> regionPathList = this.getRegionList(tableArchivePath);
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
   * Counts the number of files in all subdirectories of an HBase table, i.e. HFiles.
   * @param regionPath Path to an HBase table directory
   * @return the number of files all directories
   * @throws IOException exception
   */
  public int getNumberOfFilesInDir(Path regionPath) throws IOException {
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
   * Duplicate the backup image if it's on local cluster
   * @see HStore#bulkLoadHFile(String, long)
   * @see HRegionFileSystem#bulkLoadStoreFile(String familyName, Path srcPath, long seqNum)
   * @param tableArchivePath archive path
   * @return the new tableArchivePath
   * @throws IOException exception
   */
  public Path checkLocalAndBackup(Path tableArchivePath) throws IOException {
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

  /**
   * Calculate region boundaries and add all the column families to the table descriptor
   * @param regionDirList region dir list
   * @return a set of keys to store the boundaries
   */
  public byte[][] generateBoundaryKeys(ArrayList<Path> regionDirList)
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
   * Check whether the backup image path and there is manifest file in the path.
   * @param backupManifestMap If all the manifests are found, then they are put into this map
   * @param tableArray the tables involved
   * @throws IOException exception
   */
  public static void checkImageManifestExist(HashMap<TableName, BackupManifest> backupManifestMap,
      TableName[] tableArray, Configuration conf,
      Path backupRootPath, String backupId) throws IOException {
    for (TableName tableName : tableArray) {
      BackupManifest manifest = getManifest(tableName, conf, backupRootPath, backupId);
      backupManifestMap.put(tableName, manifest);
    }
  }
}