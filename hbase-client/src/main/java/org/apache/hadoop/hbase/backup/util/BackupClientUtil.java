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
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupRestoreConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A collection of methods used by multiple classes to backup HBase tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupClientUtil {
  protected static final Log LOG = LogFactory.getLog(BackupClientUtil.class);
  public static final String LOGNAME_SEPARATOR = ".";

  private BackupClientUtil() {
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Check whether the backup path exist
   * @param backupStr backup
   * @param conf configuration
   * @return Yes if path exists
   * @throws IOException exception
   */
  public static boolean checkPathExist(String backupStr, Configuration conf) throws IOException {
    boolean isExist = false;
    Path backupPath = new Path(backupStr);
    FileSystem fileSys = backupPath.getFileSystem(conf);
    String targetFsScheme = fileSys.getUri().getScheme();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Schema of given url: " + backupStr + " is: " + targetFsScheme);
    }
    if (fileSys.exists(backupPath)) {
      isExist = true;
    }
    return isExist;
  }

  // check target path first, confirm it doesn't exist before backup
  public static void checkTargetDir(String backupRootPath, Configuration conf) throws IOException {
    boolean targetExists = false;
    try {
      targetExists = checkPathExist(backupRootPath, conf);
    } catch (IOException e) {
      String expMsg = e.getMessage();
      String newMsg = null;
      if (expMsg.contains("No FileSystem for scheme")) {
        newMsg =
            "Unsupported filesystem scheme found in the backup target url. Error Message: "
                + newMsg;
        LOG.error(newMsg);
        throw new IOException(newMsg);
      } else {
        throw e;
      }
    }

    if (targetExists) {
      LOG.info("Using existing backup root dir: " + backupRootPath);
    } else {
      LOG.info("Backup root dir " + backupRootPath + " does not exist. Will be created.");
    }
  }

  /**
   * Get the min value for all the Values a map.
   * @param map map
   * @return the min value
   */
  public static <T> Long getMinValue(HashMap<T, Long> map) {
    Long minTimestamp = null;
    if (map != null) {
      ArrayList<Long> timestampList = new ArrayList<Long>(map.values());
      Collections.sort(timestampList);
      // The min among all the RS log timestamps will be kept in hbase:backup table.
      minTimestamp = timestampList.get(0);
    }
    return minTimestamp;
  }

  /**
   * Parses host name:port from archived WAL path
   * @param p path
   * @return host name
   * @throws IOException exception
   */
  public static String parseHostFromOldLog(Path p) {
    try {
      String n = p.getName();
      int idx = n.lastIndexOf(LOGNAME_SEPARATOR);
      String s = URLDecoder.decode(n.substring(0, idx), "UTF8");
      return ServerName.parseHostname(s) + ":" + ServerName.parsePort(s);
    } catch (Exception e) {
      LOG.warn("Skip log file (can't parse): " + p);
      return null;
    }
  }

  /**
   * Given the log file, parse the timestamp from the file name. The timestamp is the last number.
   * @param p a path to the log file
   * @return the timestamp
   * @throws IOException exception
   */
  public static Long getCreationTime(Path p) throws IOException {
    int idx = p.getName().lastIndexOf(LOGNAME_SEPARATOR);
    if (idx < 0) {
      throw new IOException("Cannot parse timestamp from path " + p);
    }
    String ts = p.getName().substring(idx + 1);
    return Long.parseLong(ts);
  }

  public static List<String> getFiles(FileSystem fs, Path rootDir, List<String> files,
      PathFilter filter) throws FileNotFoundException, IOException {
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(rootDir, true);

    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (lfs.isDirectory()) {
        continue;
      }
      // apply filter
      if (filter.accept(lfs.getPath())) {
        files.add(lfs.getPath().toString());
      }
    }
    return files;
  }

  public static void cleanupBackupData(BackupInfo context, Configuration conf) throws IOException {
    cleanupHLogDir(context, conf);
    cleanupTargetDir(context, conf);
  }

  /**
   * Clean up directories which are generated when DistCp copying hlogs.
   * @throws IOException
   */
  private static void cleanupHLogDir(BackupInfo backupContext, Configuration conf)
      throws IOException {

    String logDir = backupContext.getHLogTargetDir();
    if (logDir == null) {
      LOG.warn("No log directory specified for " + backupContext.getBackupId());
      return;
    }

    Path rootPath = new Path(logDir).getParent();
    FileSystem fs = FileSystem.get(rootPath.toUri(), conf);
    FileStatus[] files = listStatus(fs, rootPath, null);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      LOG.debug("Delete log files: " + file.getPath().getName());
      fs.delete(file.getPath(), true);
    }
  }

  /**
   * Clean up the data at target directory
   */
  private static void cleanupTargetDir(BackupInfo backupInfo, Configuration conf) {
    try {
      // clean up the data at target directory
      LOG.debug("Trying to cleanup up target dir : " + backupInfo.getBackupId());
      String targetDir = backupInfo.getTargetRootDir();
      if (targetDir == null) {
        LOG.warn("No target directory specified for " + backupInfo.getBackupId());
        return;
      }

      FileSystem outputFs = FileSystem.get(new Path(backupInfo.getTargetRootDir()).toUri(), conf);

      for (TableName table : backupInfo.getTables()) {
        Path targetDirPath =
            new Path(getTableBackupDir(backupInfo.getTargetRootDir(), backupInfo.getBackupId(),
              table));
        if (outputFs.delete(targetDirPath, true)) {
          LOG.info("Cleaning up backup data at " + targetDirPath.toString() + " done.");
        } else {
          LOG.info("No data has been found in " + targetDirPath.toString() + ".");
        }

        Path tableDir = targetDirPath.getParent();
        FileStatus[] backups = listStatus(outputFs, tableDir, null);
        if (backups == null || backups.length == 0) {
          outputFs.delete(tableDir, true);
          LOG.debug(tableDir.toString() + " is empty, remove it.");
        }
      }
      outputFs.delete(new Path(targetDir, backupInfo.getBackupId()), true);
    } catch (IOException e1) {
      LOG.error("Cleaning up backup data of " + backupInfo.getBackupId() + " at "
          + backupInfo.getTargetRootDir() + " failed due to " + e1.getMessage() + ".");
    }
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup1/backup_1396650096738/default/t1_dn/"
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @param table table name
   * @return backupPath String for the particular table
   */
  public static String
      getTableBackupDir(String backupRootDir, String backupId, TableName tableName) {
    return backupRootDir + Path.SEPARATOR + backupId + Path.SEPARATOR
        + tableName.getNamespaceAsString() + Path.SEPARATOR + tableName.getQualifierAsString()
        + Path.SEPARATOR;
  }

  public static TableName[] parseTableNames(String tables) {
    if (tables == null) {
      return null;
    }
    String[] tableArray = tables.split(BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);

    TableName[] ret = new TableName[tableArray.length];
    for (int i = 0; i < tableArray.length; i++) {
      ret[i] = TableName.valueOf(tableArray[i]);
    }
    return ret;
  }

  /**
   * Sort history list by start time in descending order.
   * @param historyList history list
   * @return sorted list of BackupCompleteData
   */
  public static ArrayList<BackupInfo> sortHistoryListDesc(ArrayList<BackupInfo> historyList) {
    ArrayList<BackupInfo> list = new ArrayList<BackupInfo>();
    TreeMap<String, BackupInfo> map = new TreeMap<String, BackupInfo>();
    for (BackupInfo h : historyList) {
      map.put(Long.toString(h.getStartTs()), h);
    }
    Iterator<String> i = map.descendingKeySet().iterator();
    while (i.hasNext()) {
      list.add(map.get(i.next()));
    }
    return list;
  }

  /**
   * Returns WAL file name
   * @param walFileName WAL file name
   * @return WAL file name
   * @throws IOException exception
   * @throws IllegalArgumentException exception
   */
  public static String getUniqueWALFileNamePart(String walFileName) throws IOException {
    return getUniqueWALFileNamePart(new Path(walFileName));
  }

  /**
   * Returns WAL file name
   * @param p - WAL file path
   * @return WAL file name
   * @throws IOException exception
   */
  public static String getUniqueWALFileNamePart(Path p) throws IOException {
    return p.getName();
  }

  /**
   * Calls fs.listStatus() and treats FileNotFoundException as non-fatal This accommodates
   * differences between hadoop versions, where hadoop 1 does not throw a FileNotFoundException, and
   * return an empty FileStatus[] while Hadoop 2 will throw FileNotFoundException.
   * @param fs file system
   * @param dir directory
   * @param filter path filter
   * @return null if dir is empty or doesn't exist, otherwise FileStatus array
   */
  public static FileStatus[]
      listStatus(final FileSystem fs, final Path dir, final PathFilter filter) throws IOException {
    FileStatus[] status = null;
    try {
      status = filter == null ? fs.listStatus(dir) : fs.listStatus(dir, filter);
    } catch (FileNotFoundException fnfe) {
      // if directory doesn't exist, return null
      if (LOG.isTraceEnabled()) {
        LOG.trace(dir + " doesn't exist");
      }
    }
    if (status == null || status.length < 1) return null;
    return status;
  }

  /**
   * Return the 'path' component of a Path. In Hadoop, Path is an URI. This method returns the
   * 'path' component of a Path's URI: e.g. If a Path is
   * <code>hdfs://example.org:9000/hbase_trunk/TestTable/compaction.dir</code>, this method returns
   * <code>/hbase_trunk/TestTable/compaction.dir</code>. This method is useful if you want to print
   * out a Path without qualifying Filesystem instance.
   * @param p Filesystem Path whose 'path' component we are to return.
   * @return Path portion of the Filesystem
   */
  public static String getPath(Path p) {
    return p.toUri().getPath();
  }

  /**
   * Given the backup root dir and the backup id, return the log file location for an incremental
   * backup.
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @return logBackupDir: ".../user/biadmin/backup1/WALs/backup_1396650096738"
   */
  public static String getLogBackupDir(String backupRootDir, String backupId) {
    return backupRootDir + Path.SEPARATOR + backupId + Path.SEPARATOR
        + HConstants.HREGION_LOGDIR_NAME;
  }

  public static List<BackupInfo> getHistory(Configuration conf, Path backupRootPath)
      throws IOException {
    // Get all (n) history from backup root destination
    FileSystem fs = FileSystem.get(conf);
    RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(backupRootPath);

    List<BackupInfo> infos = new ArrayList<BackupInfo>();
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (!lfs.isDirectory()) continue;
      if (!isBackupDirectory(lfs)) continue;
      String backupId = lfs.getPath().getName();
      infos.add(loadBackupInfo(backupRootPath, backupId, fs));
    }
    // Sort
    Collections.sort(infos, new Comparator<BackupInfo>() {

      @Override
      public int compare(BackupInfo o1, BackupInfo o2) {
        long ts1 = getTimestamp(o1.getBackupId());
        long ts2 = getTimestamp(o2.getBackupId());
        if (ts1 == ts2) return 0;
        return ts1 < ts2 ? 1 : -1;
      }

      private long getTimestamp(String backupId) {
        String[] split = backupId.split("_");
        return Long.parseLong(split[1]);
      }
    });
    return infos;
  }

  public static List<BackupInfo> getHistory(Configuration conf, int n, TableName name,
      Path backupRootPath) throws IOException {
    List<BackupInfo> infos = getHistory(conf, backupRootPath);
    if (name == null) {
      if (infos.size() <= n) return infos;
      return infos.subList(0, n);
    } else {
      List<BackupInfo> ret = new ArrayList<BackupInfo>();
      int count = 0;
      for (BackupInfo info : infos) {
        List<TableName> names = info.getTableNames();
        if (names.contains(name)) {
          ret.add(info);
          if (++count == n) {
            break;
          }
        }
      }
      return ret;
    }
  }

  private static boolean isBackupDirectory(LocatedFileStatus lfs) {
    return lfs.getPath().getName().startsWith(BackupRestoreConstants.BACKUPID_PREFIX);
  }

  public static BackupInfo loadBackupInfo(Path backupRootPath, String backupId, FileSystem fs)
      throws IOException {
    Path backupPath = new Path(backupRootPath, backupId);

    RemoteIterator<LocatedFileStatus> it = fs.listFiles(backupPath, true);
    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (lfs.getPath().getName().equals(BackupManifest.MANIFEST_FILE_NAME)) {
        // Load BackupManifest
        BackupManifest manifest = new BackupManifest(fs, lfs.getPath().getParent());
        BackupInfo info = manifest.toBackupInfo();
        return info;
      }
    }
    return null;
  }
}
