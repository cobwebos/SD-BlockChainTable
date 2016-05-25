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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos;

/**
 * This class provides 'hbase:backup' table API
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupSystemTable implements Closeable {
  
  static class WALItem {
    String backupId;
    String walFile;
    String backupRoot;
    
    WALItem(String backupId, String walFile, String backupRoot)
    {
      this.backupId = backupId;
      this.walFile = walFile;
      this.backupRoot = backupRoot;
    }

    public String getBackupId() {
      return backupId;
    }

    public String getWalFile() {
      return walFile;
    }

    public String getBackupRoot() {
      return backupRoot;
    }
    
    public String toString() {
      return "/"+ backupRoot + "/"+backupId + "/" + walFile;
    }
    
  }
  
  private static final Log LOG = LogFactory.getLog(BackupSystemTable.class);
  private final static TableName tableName = TableName.BACKUP_TABLE_NAME;  
  // Stores backup sessions (contexts)
  final static byte[] SESSIONS_FAMILY = "session".getBytes();
  // Stores other meta 
  final static byte[] META_FAMILY = "meta".getBytes();
  // Connection to HBase cluster, shared
  // among all instances
  private final Connection connection;
    
  public BackupSystemTable(Connection conn) throws IOException {
    this.connection = conn;
  }

 
  public void close() {
     // do nothing 
  }

  /**
   * Updates status (state) of a backup session in hbase:backup table
   * @param context context
   * @throws IOException exception
   */
  public void updateBackupInfo(BackupInfo context) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("update backup status in hbase:backup for: " + context.getBackupId()
        + " set status=" + context.getState());
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = BackupSystemTableHelper.createPutForBackupContext(context);
      table.put(put);
    }
  }

  /**
   * Deletes backup status from hbase:backup table
   * @param backupId backup id
   * @return true, if operation succeeded, false - otherwise 
   * @throws IOException exception
   */

  public void deleteBackupInfo(String backupId) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("delete backup status in hbase:backup for " + backupId);
    }
    try (Table table = connection.getTable(tableName)) {
      Delete del = BackupSystemTableHelper.createDeleteForBackupInfo(backupId);
      table.delete(del);
    }
  }

  /**
   * Reads backup status object (instance of BackupContext) from hbase:backup table
   * @param backupId - backupId
   * @return Current status of backup session or null
   */

  public BackupInfo readBackupInfo(String backupId) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read backup status from hbase:backup for: " + backupId);
    }

    try (Table table = connection.getTable(tableName)) {
      Get get = BackupSystemTableHelper.createGetForBackupContext(backupId);
      Result res = table.get(get);
      if(res.isEmpty()){
        return null;
      }
      return BackupSystemTableHelper.resultToBackupInfo(res);
    }
  }

  /**
   * Read the last backup start code (timestamp) of last successful backup. Will return null if
   * there is no start code stored on hbase or the value is of length 0. These two cases indicate
   * there is no successful backup completed so far.
   * @param backupRoot root directory path to backup 
   * @return the timestamp of last successful backup
   * @throws IOException exception
   */
  public String readBackupStartCode(String backupRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read backup start code from hbase:backup");
    }
    try (Table table = connection.getTable(tableName)) {
      Get get = BackupSystemTableHelper.createGetForStartCode(backupRoot);
      Result res = table.get(get);
      if (res.isEmpty()) {
        return null;
      }
      Cell cell = res.listCells().get(0);
      byte[] val = CellUtil.cloneValue(cell);
      if (val.length == 0){
        return null;
      }
      return new String(val);
    }
  }

  /**
   * Write the start code (timestamp) to hbase:backup. If passed in null, then write 0 byte.
   * @param startCode start code
   * @param backupRoot root directory path to backup 
   * @throws IOException exception
   */
  public void writeBackupStartCode(Long startCode, String backupRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write backup start code to hbase:backup " + startCode);
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = BackupSystemTableHelper.createPutForStartCode(startCode.toString(), backupRoot);
      table.put(put);
    }
  }

  /**
   * Get the Region Servers log information after the last log roll from hbase:backup.
   * @param backupRoot root directory path to backup 
   * @return RS log info
   * @throws IOException exception
   */
  public HashMap<String, Long> readRegionServerLastLogRollResult(String backupRoot)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read region server last roll log result to hbase:backup");
    }

    Scan scan = BackupSystemTableHelper.createScanForReadRegionServerLastLogRollResult(backupRoot);

    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      Result res = null;
      HashMap<String, Long> rsTimestampMap = new HashMap<String, Long>();
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String server =
            BackupSystemTableHelper.getServerNameForReadRegionServerLastLogRollResult(row);
        byte[] data = CellUtil.cloneValue(cell);
        rsTimestampMap.put(server, Long.parseLong(new String(data)));
      }
      return rsTimestampMap;
    }
  }

  /**
   * Writes Region Server last roll log result (timestamp) to hbase:backup table
   * @param server - Region Server name
   * @param timestamp - last log timestamp
   * @param backupRoot root directory path to backup 
   * @throws IOException exception
   */
  public void writeRegionServerLastLogRollResult(String server, Long ts, String backupRoot)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write region server last roll log result to hbase:backup");
    }
    try (Table table = connection.getTable(tableName)) {
      Put put =
          BackupSystemTableHelper.createPutForRegionServerLastLogRollResult(server,ts,backupRoot);
      table.put(put);
    }
  }

  /**
   * Get all completed backup information (in desc order by time)
   * @param onlyCompeleted, true, if only successfully completed sessions
   * @return history info of BackupCompleteData
   * @throws IOException exception
   */
  public ArrayList<BackupInfo> getBackupHistory(boolean onlyCompleted) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get backup history from hbase:backup");
    }
    ArrayList<BackupInfo> list ;
    BackupState state = onlyCompleted? BackupState.COMPLETE: BackupState.ANY;
    list = getBackupContexts(state);
    return BackupClientUtil.sortHistoryListDesc(list);    
  }

  public ArrayList<BackupInfo> getBackupHistory() throws IOException {
    return getBackupHistory(false);
  }
  
  /**
   * Get all backup session with a given status (in desc order by time)
   * @param status status
   * @return history info of backup contexts
   * @throws IOException exception
   */
  public ArrayList<BackupInfo> getBackupContexts(BackupState status) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get backup contexts from hbase:backup");
    }

    Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
    ArrayList<BackupInfo> list = new ArrayList<BackupInfo>();

    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        BackupInfo context = BackupSystemTableHelper.cellToBackupInfo(res.current());
        if (status != BackupState.ANY && context.getState() != status){
          continue;
        }
        list.add(context);
      }
      return list;
    }
  }

  /**
   * Write the current timestamps for each regionserver to hbase:backup 
   * after a successful full or incremental backup. The saved timestamp is of the last
   *  log file that was backed up already.
   * @param tables tables
   * @param newTimestamps timestamps
   * @param backupRoot root directory path to backup 
   * @throws IOException exception
   */
  public void writeRegionServerLogTimestamp(Set<TableName> tables,
      HashMap<String, Long> newTimestamps, String backupRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write RS log time stamps to hbase:backup for tables ["+ 
          StringUtils.join(tables, ",")+"]");
    }
    List<Put> puts = new ArrayList<Put>();
    for (TableName table : tables) {
      byte[] smapData = toTableServerTimestampProto(table, newTimestamps).toByteArray();
      Put put = 
          BackupSystemTableHelper.createPutForWriteRegionServerLogTimestamp(table, 
            smapData, backupRoot);
      puts.add(put);
    }
    try (Table table = connection.getTable(tableName)) {
      table.put(puts);
    }
  }

  /**
   * Read the timestamp for each region server log after the last successful backup. Each table has
   * its own set of the timestamps. The info is stored for each table as a concatenated string of
   * rs->timestapmp
   * @param backupRoot root directory path to backup 
   * @return the timestamp for each region server. key: tableName value:
   *         RegionServer,PreviousTimeStamp
   * @throws IOException exception
   */
  public HashMap<TableName, HashMap<String, Long>> readLogTimestampMap(String backupRoot)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read RS log ts from hbase:backup for root="+ backupRoot);
    }

    HashMap<TableName, HashMap<String, Long>> tableTimestampMap =
        new HashMap<TableName, HashMap<String, Long>>();

    Scan scan = BackupSystemTableHelper.createScanForReadLogTimestampMap(backupRoot);
    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String tabName = BackupSystemTableHelper.getTableNameForReadLogTimestampMap(row);
        TableName tn = TableName.valueOf(tabName);
        byte[] data = CellUtil.cloneValue(cell);
        if (data == null) {
          throw new IOException("Data of last backup data from hbase:backup "
              + "is empty. Create a backup first.");
        }
        if (data != null && data.length > 0) {
          HashMap<String, Long> lastBackup =
              fromTableServerTimestampProto(BackupProtos.TableServerTimestamp.parseFrom(data));
          tableTimestampMap.put(tn, lastBackup);
        }
      }
      return tableTimestampMap;
    }
  }

  private BackupProtos.TableServerTimestamp toTableServerTimestampProto(TableName table,
      Map<String, Long> map) {
    BackupProtos.TableServerTimestamp.Builder tstBuilder =
        BackupProtos.TableServerTimestamp.newBuilder();
    tstBuilder.setTable(ProtobufUtil.toProtoTableName(table));

    for(Entry<String, Long> entry: map.entrySet()) {
      BackupProtos.ServerTimestamp.Builder builder = BackupProtos.ServerTimestamp.newBuilder();
      builder.setServer(entry.getKey());
      builder.setTimestamp(entry.getValue());
      tstBuilder.addServerTimestamp(builder.build());
    }

    return tstBuilder.build();
  }

  private HashMap<String, Long> fromTableServerTimestampProto(
      BackupProtos.TableServerTimestamp proto) {
    HashMap<String, Long> map = new HashMap<String, Long> ();
    List<BackupProtos.ServerTimestamp> list = proto.getServerTimestampList();
    for(BackupProtos.ServerTimestamp st: list) {
      map.put(st.getServer(), st.getTimestamp());
    }
    return map;
  }

  /**
   * Return the current tables covered by incremental backup.
   * @param backupRoot root directory path to backup 
   * @return set of tableNames
   * @throws IOException exception
   */
  public  Set<TableName> getIncrementalBackupTableSet(String backupRoot)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get incr backup table set from hbase:backup");
    }
    TreeSet<TableName> set = new TreeSet<>();

    try (Table table = connection.getTable(tableName)) {
      Get get = BackupSystemTableHelper.createGetForIncrBackupTableSet(backupRoot);
      Result res = table.get(get);
      if (res.isEmpty()) {
        return set;
      }
      List<Cell> cells = res.listCells();
      for (Cell cell : cells) {
        // qualifier = table name - we use table names as qualifiers
        set.add(TableName.valueOf(CellUtil.cloneQualifier(cell)));
      }
      return set;
    }
  }

  /**
   * Add tables to global incremental backup set
   * @param tables - set of tables
   * @param backupRoot root directory path to backup 
   * @throws IOException exception
   */
  public void addIncrementalBackupTableSet(Set<TableName> tables, String backupRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Add incremental backup table set to hbase:backup. ROOT="+backupRoot +
        " tables ["+ StringUtils.join(tables, " ")+"]");
      for (TableName table : tables) {
        LOG.debug(table);
      }
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = BackupSystemTableHelper.createPutForIncrBackupTableSet(tables, backupRoot);
      table.put(put);
    }
  }

  /**
   * Register WAL files as eligible for deletion
   * @param files files
   * @param backupId backup id
   * @param backupRoot root directory path to backup 
   * @throws IOException exception
   */
  public void addWALFiles(List<String> files, String backupId, 
      String backupRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("add WAL files to hbase:backup: "+backupId +" "+backupRoot+" files ["+
     StringUtils.join(files, ",")+"]");
      for(String f: files){
        LOG.debug("add :"+f);
      }
    }
    try (Table table = connection.getTable(tableName)) {
      List<Put> puts = 
          BackupSystemTableHelper.createPutsForAddWALFiles(files, backupId, backupRoot);
      table.put(puts);
    }
  }

  /**
   * Register WAL files as eligible for deletion
   * @param backupRoot root directory path to backup 
   * @throws IOException exception
   */
  public Iterator<WALItem> getWALFilesIterator(String backupRoot) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get WAL files from hbase:backup");
    }
    final Table table = connection.getTable(tableName);
    Scan scan = BackupSystemTableHelper.createScanForGetWALs(backupRoot);
    final ResultScanner scanner = table.getScanner(scan);
    final Iterator<Result> it = scanner.iterator();
    return new Iterator<WALItem>() {

      @Override
      public boolean hasNext() {
        boolean next = it.hasNext();
        if (!next) {
          // close all
          try {
            scanner.close();
            table.close();
          } catch (IOException e) {
            LOG.error("Close WAL Iterator", e);
          }
        }
        return next;
      }

      @Override
      public WALItem next() {
        Result next = it.next();
        List<Cell> cells = next.listCells();
        byte[] buf = cells.get(0).getValueArray();
        int len = cells.get(0).getValueLength();
        int offset = cells.get(0).getValueOffset();
        String backupId = new String(buf, offset, len);
        buf = cells.get(1).getValueArray();
        len = cells.get(1).getValueLength();
        offset = cells.get(1).getValueOffset();
        String walFile = new String(buf, offset, len);
        buf = cells.get(2).getValueArray();
        len = cells.get(2).getValueLength();
        offset = cells.get(2).getValueOffset();
        String backupRoot = new String(buf, offset, len);    
        return new WALItem(backupId, walFile, backupRoot);
      }

      @Override
      public void remove() {
        // not implemented
        throw new RuntimeException("remove is not supported");
      }
    };

  }

  /**
   * Check if WAL file is eligible for deletion
   * Future: to support all backup destinations
   * @param file file
   * @return true, if - yes.
   * @throws IOException exception
   */
  public boolean isWALFileDeletable(String file) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Check if WAL file has been already backed up in hbase:backup "+ file);
    }
    try (Table table = connection.getTable(tableName)) {
      Get get = BackupSystemTableHelper.createGetForCheckWALFile(file);
      Result res = table.get(get);
      if (res.isEmpty()){
        return false;
      }
      return true;
    }
  }

  /**
   * Checks if we have at least one backup session in hbase:backup This API is used by
   * BackupLogCleaner
   * @return true, if - at least one session exists in hbase:backup table
   * @throws IOException exception
   */
  public boolean hasBackupSessions() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Has backup sessions from hbase:backup");
    }
    boolean result = false;
    Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
    scan.setCaching(1);
    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      if (scanner.next() != null) {
        result = true;
      }
      return result;
    }
  }
  
  /**
   * BACKUP SETS
   */
  
  /**
   * Get backup set list
   * @return backup set list
   * @throws IOException
   */
  public List<String> listBackupSets() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(" Backup set list");
    }
    List<String> list = new ArrayList<String>();
    Table table = null;
    ResultScanner scanner = null;
    try {
      table = connection.getTable(tableName);
      Scan scan = BackupSystemTableHelper.createScanForBackupSetList();
      scan.setMaxVersions(1);
      scanner = table.getScanner(scan);
      Result res = null;
     while ((res = scanner.next()) != null) {
       res.advance();
       list.add(BackupSystemTableHelper.cellKeyToBackupSetName(res.current()));
     }
     return list;
   } finally {
     if(scanner != null) {
       scanner.close();
     }
     if (table != null) {
       table.close();
     }
   }
 }
 
 /**
  * Get backup set description (list of tables)
  * @param name - set's name
  * @return list of tables in a backup set 
  * @throws IOException
  */
 public List<TableName> describeBackupSet(String name) throws IOException {
   if (LOG.isDebugEnabled()) {
     LOG.debug(" Backup set describe: "+name);
   }
   Table table = null;
   try {
     table = connection.getTable(tableName);
     Get get = BackupSystemTableHelper.createGetForBackupSet(name);
     Result res = table.get(get);
     if(res.isEmpty()) return new ArrayList<TableName>();
     res.advance();
     String[] tables = 
         BackupSystemTableHelper.cellValueToBackupSet(res.current());
     return toList(tables);
   } finally {
     if (table != null) {
       table.close();
     }
   }
 }
 
 private List<TableName> toList(String[] tables)
 {
   List<TableName> list = new ArrayList<TableName>(tables.length);
   for(String name: tables) {
     list.add(TableName.valueOf(name));
   }
   return list;
 }
 
 /**
  * Add backup set (list of tables)
  * @param name - set name
  * @param tables - list of tables, comma-separated
  * @throws IOException
  */
 public void addToBackupSet(String name, String[] newTables) throws IOException {
   if (LOG.isDebugEnabled()) {
     LOG.debug("Backup set add: "+name+" tables ["+ StringUtils.join(newTables, " ")+"]");
   }
   Table table = null;
   String[] union = null;
   try {
     table = connection.getTable(tableName);
     Get get = BackupSystemTableHelper.createGetForBackupSet(name);
     Result res = table.get(get);
     if(res.isEmpty()) {
       union = newTables;
     } else {
       res.advance();
       String[] tables = 
         BackupSystemTableHelper.cellValueToBackupSet(res.current());
       union = merge(tables, newTables);  
     }
     Put put = BackupSystemTableHelper.createPutForBackupSet(name, union);
     table.put(put);
   } finally {
     if (table != null) {
       table.close();
     }
   }
 }
 
 private String[] merge(String[] tables, String[] newTables) {
   List<String> list = new ArrayList<String>();
   // Add all from tables
   for(String t: tables){
     list.add(t);
   }
   for(String nt: newTables){
     if(list.contains(nt)) continue;
     list.add(nt);
   }
   String[] arr = new String[list.size()];
   list.toArray(arr);
   return arr;
 }

 /**
  * Remove tables from backup set (list of tables)
  * @param name - set name
  * @param tables - list of tables, comma-separated
  * @throws IOException
  */
  public void removeFromBackupSet(String name, String[] toRemove) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(" Backup set remove from : " + name+" tables ["+
     StringUtils.join(toRemove, " ")+"]");
    }
    Table table = null;
    String[] disjoint = null;
    try {
      table = connection.getTable(tableName);
      Get get = BackupSystemTableHelper.createGetForBackupSet(name);
      Result res = table.get(get);
      if (res.isEmpty()) {
        LOG.warn("Backup set '"+ name+"' not found.");
        return;
      } else {
        res.advance();
        String[] tables = BackupSystemTableHelper.cellValueToBackupSet(res.current());
        disjoint = disjoin(tables, toRemove);
      }
      if (disjoint.length > 0) {
        Put put = BackupSystemTableHelper.createPutForBackupSet(name, disjoint);
        table.put(put);
      } else {
        // Delete
        //describeBackupSet(name);
        LOG.warn("Backup set '"+ name+"' does not contain tables ["+
        StringUtils.join(toRemove, " ")+"]");
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private String[] disjoin(String[] tables, String[] toRemove) {
    List<String> list = new ArrayList<String>();
    // Add all from tables
    for (String t : tables) {
      list.add(t);
    }
    for (String nt : toRemove) {
      if (list.contains(nt)) {
        list.remove(nt);
      }
    }
    String[] arr = new String[list.size()];
    list.toArray(arr);
    return arr;
  }

 /**
  * Delete backup set 
  * @param name set's name
  * @throws IOException
  */
  public void deleteBackupSet(String name) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(" Backup set delete: " + name);
    }
    Table table = null;
    try {
      table = connection.getTable(tableName);
      Delete del = BackupSystemTableHelper.createDeleteForBackupSet(name);
      table.delete(del);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * Get backup system table descriptor
   * @return descriptor
   */
  public static HTableDescriptor getSystemTableDescriptor() {
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor colSessionsDesc = new HColumnDescriptor(SESSIONS_FAMILY);
    colSessionsDesc.setMaxVersions(1);
    // Time to keep backup sessions (secs)
    Configuration config = HBaseConfiguration.create();
    int ttl =
        config.getInt(HConstants.BACKUP_SYSTEM_TTL_KEY, HConstants.BACKUP_SYSTEM_TTL_DEFAULT);
    colSessionsDesc.setTimeToLive(ttl);
    tableDesc.addFamily(colSessionsDesc);
    HColumnDescriptor colMetaDesc = new HColumnDescriptor(META_FAMILY);
    //colDesc.setMaxVersions(1);
    tableDesc.addFamily(colMetaDesc);
    return tableDesc;
  }

  public static String getTableNameAsString() {
    return tableName.getNameAsString();
  }
  
  public static TableName getTableName() {
    return tableName;
  }
}
