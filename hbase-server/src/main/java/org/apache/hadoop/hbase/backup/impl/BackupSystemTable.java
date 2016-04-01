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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupContext.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupUtil.BackupCompleteData;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
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

  private static final Log LOG = LogFactory.getLog(BackupSystemTable.class);
  private final static TableName tableName = TableName.BACKUP_TABLE_NAME;
  final static byte[] familyName = "f".getBytes();

  // Connection to HBase cluster, shared
  // among all instances
  private final Connection connection;
  // Cluster configuration
  private final Configuration conf;

  /**
   * Create a BackupSystemTable object for the given Connection. Connection is NOT owned by this
   * instance and has to be closed explicitly.
   * @param connection
   * @throws IOException
   */
  public BackupSystemTable(Connection connection) throws IOException {
    this.connection = connection;
    this.conf = connection.getConfiguration();

    createSystemTableIfNotExists();
  }

  @Override
  public void close() {
  }

  private void createSystemTableIfNotExists() throws IOException {
    try(Admin admin = connection.getAdmin()) {
      if (admin.tableExists(tableName) == false) {
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor colDesc = new HColumnDescriptor(familyName);
        colDesc.setMaxVersions(1);
        int ttl =
            conf.getInt(HConstants.BACKUP_SYSTEM_TTL_KEY, HConstants.BACKUP_SYSTEM_TTL_DEFAULT);
        colDesc.setTimeToLive(ttl);
        tableDesc.addFamily(colDesc);
        admin.createTable(tableDesc);
      }
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    }
  }

  /**
   * Updates status (state) of a backup session in hbase:backup table
   * @param context context
   * @throws IOException exception
   */
  public void updateBackupStatus(BackupContext context) throws IOException {

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
   * @throws IOException exception
   */

  public void deleteBackupStatus(String backupId) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("delete backup status in hbase:backup for " + backupId);
    }
    try (Table table = connection.getTable(tableName)) {
      Delete del = BackupSystemTableHelper.createDeletForBackupContext(backupId);
      table.delete(del);
    }
  }

  /**
   * Reads backup status object (instance of BackupContext) from hbase:backup table
   * @param backupId - backupId
   * @return Current status of backup session or null
   */

  public BackupContext readBackupStatus(String backupId) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read backup status from hbase:backup for: " + backupId);
    }

    try (Table table = connection.getTable(tableName)) {
      Get get = BackupSystemTableHelper.createGetForBackupContext(backupId);
      Result res = table.get(get);
      if(res.isEmpty()){
        return null;
      }
      return BackupSystemTableHelper.resultToBackupContext(res);
    }
  }

  /**
   * Read the last backup start code (timestamp) of last successful backup. Will return null if
   * there is no start code stored on hbase or the value is of length 0. These two cases indicate
   * there is no successful backup completed so far.
   * @return the timestamp of last successful backup
   * @throws IOException exception
   */
  public String readBackupStartCode() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read backup start code from hbase:backup");
    }
    try (Table table = connection.getTable(tableName)) {
      Get get = BackupSystemTableHelper.createGetForStartCode();
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
   * @throws IOException exception
   */
  public void writeBackupStartCode(Long startCode) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write backup start code to hbase:backup " + startCode);
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = BackupSystemTableHelper.createPutForStartCode(startCode.toString());
      table.put(put);
    }
  }

  /**
   * Get the Region Servers log information after the last log roll from hbase:backup.
   * @return RS log info
   * @throws IOException exception
   */
  public HashMap<String, Long> readRegionServerLastLogRollResult()
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read region server last roll log result to hbase:backup");
    }

    Scan scan = BackupSystemTableHelper.createScanForReadRegionServerLastLogRollResult();
    scan.setMaxVersions(1);

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
   * @throws IOException exception
   */
  public void writeRegionServerLastLogRollResult(String server, Long timestamp)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write region server last roll log result to hbase:backup");
    }
    try (Table table = connection.getTable(tableName)) {
      Put put =
          BackupSystemTableHelper.createPutForRegionServerLastLogRollResult(server, timestamp);
      table.put(put);
    }
  }

  /**
   * Get all completed backup information (in desc order by time)
   * @return history info of BackupCompleteData
   * @throws IOException exception
   */
  public ArrayList<BackupCompleteData> getBackupHistory() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get backup history from hbase:backup");
    }
    Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
    scan.setMaxVersions(1);

    ArrayList<BackupCompleteData> list = new ArrayList<BackupCompleteData>();
    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {

      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        BackupContext context = BackupSystemTableHelper.cellToBackupContext(res.current());
        if (context.getState() != BackupState.COMPLETE) {
          continue;
        }

        BackupCompleteData history = new BackupCompleteData();
        history.setBackupToken(context.getBackupId());
        history.setStartTime(Long.toString(context.getStartTs()));
        history.setEndTime(Long.toString(context.getEndTs()));
        history.setBackupRootPath(context.getTargetRootDir());
        history.setTableList(context.getTableNames());
        history.setType(context.getType().toString());
        history.setBytesCopied(Long.toString(context.getTotalBytesCopied()));

        list.add(history);
      }
      return BackupUtil.sortHistoryListDesc(list);
    }
  }

  /**
   * Get all backup session with a given status (in desc order by time)
   * @param status status
   * @return history info of backup contexts
   * @throws IOException exception
   */
  public ArrayList<BackupContext> getBackupContexts(BackupState status) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get backup contexts from hbase:backup");
    }

    Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
    scan.setMaxVersions(1);
    ArrayList<BackupContext> list = new ArrayList<BackupContext>();

    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        BackupContext context = BackupSystemTableHelper.cellToBackupContext(res.current());
        if (context.getState() != status){
          continue;
        }
        list.add(context);
      }
      return list;
    }
  }

  /**
   * Write the current timestamps for each regionserver to hbase:backup after a successful full or
   * incremental backup. The saved timestamp is of the last log file that was backed up already.
   * @param tables tables
   * @param newTimestamps timestamps
   * @throws IOException exception
   */
  public void writeRegionServerLogTimestamp(Set<TableName> tables,
      HashMap<String, Long> newTimestamps) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write RS log ts to HBASE_BACKUP");
    }
    List<Put> puts = new ArrayList<Put>();
    for (TableName table : tables) {
      byte[] smapData = toTableServerTimestampProto(table, newTimestamps).toByteArray();
      Put put = BackupSystemTableHelper.createPutForWriteRegionServerLogTimestamp(table, smapData);
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
   * @return the timestamp for each region server. key: tableName value:
   *         RegionServer,PreviousTimeStamp
   * @throws IOException exception
   */
  public HashMap<TableName, HashMap<String, Long>> readLogTimestampMap() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("read RS log ts from HBASE_BACKUP");
    }

    HashMap<TableName, HashMap<String, Long>> tableTimestampMap =
        new HashMap<TableName, HashMap<String, Long>>();

    Scan scan = BackupSystemTableHelper.createScanForReadLogTimestampMap();
    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      Result res = null;
      while ((res = scanner.next()) != null) {
        res.advance();
        Cell cell = res.current();
        byte[] row = CellUtil.cloneRow(cell);
        String tabName = BackupSystemTableHelper.getTableNameForReadLogTimestampMap(row);
        TableName tn = TableName.valueOf(tabName);
        HashMap<String, Long> lastBackup = new HashMap<String, Long>();
        byte[] data = CellUtil.cloneValue(cell);

        if (data == null) {
          throw new IOException("Data of last backup data from HBASE_BACKUP "
              + "is empty. Create a backup first.");
        }
        if (data != null && data.length > 0) {
          lastBackup =
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
   * Add tables to global incremental backup set
   * @param tables - set of tables
   * @throws IOException exception
   */
  public void addIncrementalBackupTableSet(Set<TableName> tables) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("add incr backup table set to hbase:backup");
      for (TableName table : tables) {
        LOG.debug(table);
      }
    }
    try (Table table = connection.getTable(tableName)) {
      Put put = BackupSystemTableHelper.createPutForIncrBackupTableSet(tables);
      table.put(put);
    }
  }

  /**
   * Register WAL files as eligible for deletion
   * @param files files
   * @throws IOException exception
   */
  public void addWALFiles(List<String> files, String backupId) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("add WAL files to hbase:backup");
    }
    try (Table table = connection.getTable(tableName)) {
      List<Put> puts = BackupSystemTableHelper.createPutsForAddWALFiles(files, backupId);
      table.put(puts);
    }
  }

  /**
   * Register WAL files as eligible for deletion
   * @param files files
   * @throws IOException exception
   */
  public Iterator<String> getWALFilesIterator() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get WAL files from hbase:backup");
    }
    final Table table = connection.getTable(tableName);
    Scan scan = BackupSystemTableHelper.createScanForGetWALs();
    final ResultScanner scanner = table.getScanner(scan);
    final Iterator<Result> it = scanner.iterator();
    return new Iterator<String>() {

      @Override
      public boolean hasNext() {
        boolean next = it.hasNext();
        if (!next) {
          // close all
          try {
            scanner.close();
            table.close();
          } catch (Exception e) {
            LOG.error(e);
          }
        }
        return next;
      }

      @Override
      public String next() {
        Result next = it.next();
        List<Cell> cells = next.listCells();
        byte[] buf = cells.get(0).getValueArray();
        int len = cells.get(0).getValueLength();
        int offset = cells.get(0).getValueOffset();
        return new String(buf, offset, len);
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
   * @param file file
   * @return true, if - yes.
   * @throws IOException exception
   */
  public boolean checkWALFile(String file) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Check if WAL file has been already backuped in hbase:backup");
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
      LOG.debug("has backup sessions from hbase:backup");
    }
    boolean result = false;
    Scan scan = BackupSystemTableHelper.createScanForBackupHistory();
    scan.setMaxVersions(1);
    scan.setCaching(1);
    try (Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
      if (scanner.next() != null) {
        result = true;
      }
      return result;
    }
  }
}
