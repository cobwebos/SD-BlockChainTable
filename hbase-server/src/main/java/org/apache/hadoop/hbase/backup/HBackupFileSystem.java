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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;

/**
 * View to an on-disk Backup Image FileSytem
 * Provides the set of methods necessary to interact with the on-disk Backup Image data.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HBackupFileSystem {
  public static final Log LOG = LogFactory.getLog(HBackupFileSystem.class);

  /**
   * This is utility class.
   */
  private HBackupFileSystem() {
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup1/backup_1396650096738/default/t1_dn/"
   * @param backupRootDir backup root directory
   * @param backupId  backup id
   * @param table table name
   * @return backupPath String for the particular table
   */
  public static String getTableBackupDir(String backupRootDir, String backupId,
      TableName tableName) {
    return backupRootDir + Path.SEPARATOR+ backupId + Path.SEPARATOR +
        tableName.getNamespaceAsString() + Path.SEPARATOR
        + tableName.getQualifierAsString() + Path.SEPARATOR ;
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup_1396650096738/backup1/default/t1_dn/"
   * @param backupRootPath backup root path
   * @param tableName table name
   * @param backupId backup Id
   * @return backupPath for the particular table
   */
  public static Path getTableBackupPath(TableName tableName, Path backupRootPath, String backupId) {
    return new Path(getTableBackupDir(backupRootPath.toString(), backupId, tableName));
  }


  public static List<HRegionInfo> loadRegionInfos(TableName tableName,
    Path backupRootPath, String backupId, Configuration conf) throws IOException
  {
    Path backupTableRoot = getTableBackupPath(tableName, backupRootPath, backupId);
    FileSystem fs = backupTableRoot.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(backupTableRoot, true);
    List<HRegionInfo> infos = new ArrayList<HRegionInfo>();
    while(it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if(lfs.isFile() && lfs.getPath().toString().endsWith(HRegionFileSystem.REGION_INFO_FILE)) {
        Path regionDir = lfs.getPath().getParent();
        HRegionInfo info = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
        infos.add(info);
      }
    }

    Collections.sort(infos);
    return infos;
  }

  /**
   * Given the backup root dir and the backup id, return the log file location for an incremental
   * backup.
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @return logBackupDir: ".../user/biadmin/backup1/WALs/backup_1396650096738"
   */
  public static String getLogBackupDir(String backupRootDir, String backupId) {
    return backupRootDir + Path.SEPARATOR + backupId+ Path.SEPARATOR
        + HConstants.HREGION_LOGDIR_NAME;
  }

  public static Path getLogBackupPath(String backupRootDir, String backupId) {
    return new Path(getLogBackupDir(backupRootDir, backupId));
  }

  private static Path getManifestPath(TableName tableName, Configuration conf,
      Path backupRootPath, String backupId) throws IOException {
    Path manifestPath = new Path(getTableBackupPath(tableName, backupRootPath, backupId),
      BackupManifest.MANIFEST_FILE_NAME);

    FileSystem fs = backupRootPath.getFileSystem(conf);
    if (!fs.exists(manifestPath)) {
      // check log dir for incremental backup case
      manifestPath =
          new Path(getLogBackupDir(backupRootPath.toString(), backupId) + Path.SEPARATOR
            + BackupManifest.MANIFEST_FILE_NAME);
      if (!fs.exists(manifestPath)) {
        String errorMsg =
            "Could not find backup manifest " + BackupManifest.MANIFEST_FILE_NAME + " for " +
                backupId + ". File " + manifestPath +
                " does not exists. Did " + backupId + " correspond to previously taken backup ?";
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