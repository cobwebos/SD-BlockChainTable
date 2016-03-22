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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.backup.BackupUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

/**
 * A collection for methods used by multiple classes to backup HBase tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupUtil {
  protected static final Log LOG = LogFactory.getLog(BackupUtil.class);
  public static final String LOGNAME_SEPARATOR = ".";

  private BackupUtil(){
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Loop through the RS log timestamp map for the tables, for each RS, find the min timestamp
   * value for the RS among the tables.
   * @param rsLogTimestampMap timestamp map
   * @return the min timestamp of each RS
   */
  protected static HashMap<String, Long> getRSLogTimestampMins(
    HashMap<TableName, HashMap<String, Long>> rsLogTimestampMap) {

    if (rsLogTimestampMap == null || rsLogTimestampMap.isEmpty()) {
      return null;
    }

    HashMap<String, Long> rsLogTimestamptMins = new HashMap<String, Long>();
    HashMap<String, HashMap<TableName, Long>> rsLogTimestampMapByRS =
        new HashMap<String, HashMap<TableName, Long>>();

    for (Entry<TableName, HashMap<String, Long>> tableEntry : rsLogTimestampMap.entrySet()) {
      TableName table = tableEntry.getKey();
      HashMap<String, Long> rsLogTimestamp = tableEntry.getValue();
      for (Entry<String, Long> rsEntry : rsLogTimestamp.entrySet()) {
        String rs = rsEntry.getKey();
        Long ts = rsEntry.getValue();
        if (!rsLogTimestampMapByRS.containsKey(rs)) {
          rsLogTimestampMapByRS.put(rs, new HashMap<TableName, Long>());
          rsLogTimestampMapByRS.get(rs).put(table, ts);
        } else {
          rsLogTimestampMapByRS.get(rs).put(table, ts);
        }
      }
    }

    for (String rs : rsLogTimestampMapByRS.keySet()) {
      rsLogTimestamptMins.put(rs, BackupUtility.getMinValue(rsLogTimestampMapByRS.get(rs)));
    }

    return rsLogTimestamptMins;
  }

  /**
   * copy out Table RegionInfo into incremental backup image need to consider move this logic into
   * HBackupFileSystem
   * @param backupContext backup context
   * @param conf configuration
   * @throws IOException exception
   * @throws InterruptedException exception
   */
  protected static void copyTableRegionInfo(BackupContext backupContext, Configuration conf)
      throws IOException, InterruptedException {

    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);

    // for each table in the table set, copy out the table info and region info files in the correct
    // directory structure
    for (TableName table : backupContext.getTables()) {

      LOG.debug("Attempting to copy table info for:" + table);
      TableDescriptor orig = FSTableDescriptors.getTableDescriptorFromFs(fs, rootDir, table);

      // write a copy of descriptor to the target directory
      Path target = new Path(backupContext.getBackupStatus(table).getTargetDir());
      FileSystem targetFs = target.getFileSystem(conf);
      FSTableDescriptors descriptors =
          new FSTableDescriptors(conf, targetFs, FSUtils.getRootDir(conf));
      descriptors.createTableDescriptorForTableDirectory(target, orig, false);
      LOG.debug("Finished copying tableinfo.");

      HBaseAdmin hbadmin = null;
      // TODO: optimize
      List<HRegionInfo> regions = null;
      try(Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        regions = admin.getTableRegions(table);
      } catch (Exception e) {
        throw new BackupException(e);
      }

      // For each region, write the region info to disk
      LOG.debug("Starting to write region info for table " + table);
      for (HRegionInfo regionInfo : regions) {
        Path regionDir =
            HRegion.getRegionDir(new Path(backupContext.getBackupStatus(table).getTargetDir()),
              regionInfo);
        regionDir =
            new Path(backupContext.getBackupStatus(table).getTargetDir(), regionDir.getName());
        writeRegioninfoOnFilesystem(conf, targetFs, regionDir, regionInfo);
      }
      LOG.debug("Finished writing region info for table " + table);
    }
  }

  /**
   * Write the .regioninfo file on-disk.
   */
  public static void writeRegioninfoOnFilesystem(final Configuration conf, final FileSystem fs,
      final Path regionInfoDir, HRegionInfo regionInfo) throws IOException {
    final byte[] content = regionInfo.toDelimitedByteArray();
    Path regionInfoFile = new Path(regionInfoDir, ".regioninfo");
    // First check to get the permissions
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // Write the RegionInfo file content
    FSDataOutputStream out = FSUtils.create(conf, fs, regionInfoFile, perms, null);
    try {
      out.write(content);
    } finally {
      out.close();
    }
  }

  /**
   * TODO: return hostname:port
   * @param p
   * @return host name: port
   * @throws IOException
   */
  public static String parseHostNameFromLogFile(Path p) throws IOException {
    if (isArchivedLogFile(p)) {
      return BackupUtility.parseHostFromOldLog(p);
    } else {
      ServerName sname = DefaultWALProvider.getServerNameFromWALDirectoryName(p);
      return sname.getHostname() + ":" + sname.getPort();
    }
  }

  private static boolean isArchivedLogFile(Path p) {
    String oldLog = Path.SEPARATOR + HConstants.HREGION_OLDLOGDIR_NAME + Path.SEPARATOR;
    return p.toString().contains(oldLog);
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
   * Get the total length of files under the given directory recursively.
   * @param fs The hadoop file system
   * @param dir The target directory
   * @return the total length of files
   * @throws IOException exception
   */
  public static long getFilesLength(FileSystem fs, Path dir) throws IOException {
    long totalLength = 0;
    FileStatus[] files = FSUtils.listStatus(fs, dir);
    if (files != null) {
      for (FileStatus fileStatus : files) {
        if (fileStatus.isDirectory()) {
          totalLength += getFilesLength(fs, fileStatus.getPath());
        } else {
          totalLength += fileStatus.getLen();
        }
      }
    }
    return totalLength;
  }

  /**
   * Keep the record for dependency for incremental backup and history info p.s, we may be able to
   * merge this class into backupImage class later
   */
  public static class BackupCompleteData implements Comparable<BackupCompleteData> {
    private String startTime;
    private String endTime;
    private String type;
    private String backupRootPath;
    private List<TableName> tableList;
    private String backupToken;
    private String bytesCopied;
    private List<String> ancestors;

    public List<String> getAncestors() {
      if (this.ancestors == null) {
        this.ancestors = new ArrayList<String>();
      }
      return this.ancestors;
    }

    public void addAncestor(String backupToken) {
      this.getAncestors().add(backupToken);
    }

    public String getBytesCopied() {
      return bytesCopied;
    }

    public void setBytesCopied(String bytesCopied) {
      this.bytesCopied = bytesCopied;
    }

    public String getBackupToken() {
      return backupToken;
    }

    public void setBackupToken(String backupToken) {
      this.backupToken = backupToken;
    }

    public String getStartTime() {
      return startTime;
    }

    public void setStartTime(String startTime) {
      this.startTime = startTime;
    }

    public String getEndTime() {
      return endTime;
    }

    public void setEndTime(String endTime) {
      this.endTime = endTime;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getBackupRootPath() {
      return backupRootPath;
    }

    public void setBackupRootPath(String backupRootPath) {
      this.backupRootPath = backupRootPath;
    }

    public List<TableName> getTableList() {
      return tableList;
    }

    public void setTableList(List<TableName> tableList) {
      this.tableList = tableList;
    }

    @Override
    public int compareTo(BackupCompleteData o) {
      Long thisTS =
          new Long(this.getBackupToken().substring(this.getBackupToken().lastIndexOf("_") + 1));
      Long otherTS =
          new Long(o.getBackupToken().substring(o.getBackupToken().lastIndexOf("_") + 1));
      return thisTS.compareTo(otherTS);
    }

  }

  /**
   * Sort history list by start time in descending order.
   * @param historyList history list
   * @return sorted list of BackupCompleteData
   */
  public static ArrayList<BackupCompleteData> sortHistoryListDesc(
    ArrayList<BackupCompleteData> historyList) {
    ArrayList<BackupCompleteData> list = new ArrayList<BackupCompleteData>();
    TreeMap<String, BackupCompleteData> map = new TreeMap<String, BackupCompleteData>();
    for (BackupCompleteData h : historyList) {
      map.put(h.getStartTime(), h);
    }
    Iterator<String> i = map.descendingKeySet().iterator();
    while (i.hasNext()) {
      list.add(map.get(i.next()));
    }
    return list;
  }

  /**
   * Get list of all WAL files (WALs and archive)
   * @param c - configuration
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getListOfWALFiles(Configuration c) throws IOException {
    Path rootDir = FSUtils.getRootDir(c);
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<String>();

    FileSystem fs = FileSystem.get(c);
    logFiles = BackupUtility.getFiles(fs, logDir, logFiles, null);
    logFiles = BackupUtility.getFiles(fs, oldLogDir, logFiles, null);
    return logFiles;
  }

  /**
   * Get list of all WAL files (WALs and archive)
   * @param c - configuration
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getListOfWALFiles(Configuration c, PathFilter filter)
      throws IOException {
    Path rootDir = FSUtils.getRootDir(c);
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<String>();

    FileSystem fs = FileSystem.get(c);
    logFiles = BackupUtility.getFiles(fs, logDir, logFiles, filter);
    logFiles = BackupUtility.getFiles(fs, oldLogDir, logFiles, filter);
    return logFiles;
  }

  /**
   * Get list of all old WAL files (WALs and archive)
   * @param c - configuration
   * @return list of WAL files
   * @throws IOException exception
   */
  public static List<String> getWALFilesOlderThan(final Configuration c,
    final HashMap<String, Long> hostTimestampMap) throws IOException {
    Path rootDir = FSUtils.getRootDir(c);
    Path logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<String> logFiles = new ArrayList<String>();

    PathFilter filter = new PathFilter() {

      @Override
      public boolean accept(Path p) {
        try {
          if (DefaultWALProvider.isMetaFile(p)) {
            return false;
          }
          String host = parseHostNameFromLogFile(p);
          Long oldTimestamp = hostTimestampMap.get(host);
          Long currentLogTS = BackupUtility.getCreationTime(p);
          return currentLogTS <= oldTimestamp;
        } catch (IOException e) {
          LOG.error(e);
          return false;
        }
      }
    };
    FileSystem fs = FileSystem.get(c);
    logFiles = BackupUtility.getFiles(fs, logDir, logFiles, filter);
    logFiles = BackupUtility.getFiles(fs, oldLogDir, logFiles, filter);
    return logFiles;
  }

  public static String join(TableName[] names) {
    StringBuilder sb = new StringBuilder();
    String sep = BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND;
    for (TableName s : names) {
      sb.append(sep).append(s.getNameAsString());
    }
    return sb.toString();
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
}
