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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * A collection for methods used by BackupSystemTable.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupSystemTableHelper {

  /**
   * hbase:backup schema:
   * 1. Backup sessions rowkey= "session:" + backupId; value = serialized
   * BackupContext
   * 2. Backup start code rowkey = "startcode:" + backupRoot; value = startcode
   * 3. Incremental backup set rowkey="incrbackupset:" + backupRoot; value=[list of tables]
   * 4. Table-RS-timestamp map rowkey="trslm:"+ backupRoot+table_name; value = map[RS-> 
   * last WAL timestamp]
   * 5. RS - WAL ts map rowkey="rslogts:"+backupRoot +server; value = last WAL timestamp
   * 6. WALs recorded rowkey="wals:"+WAL unique file name; value = backupId and full WAL file name
   */

  private final static String BACKUP_INFO_PREFIX = "session:";
  private final static String START_CODE_ROW = "startcode:";
  private final static String INCR_BACKUP_SET = "incrbackupset:";
  private final static String TABLE_RS_LOG_MAP_PREFIX = "trslm:";
  private final static String RS_LOG_TS_PREFIX = "rslogts:";
  private final static String WALS_PREFIX = "wals:";
  private final static String SET_KEY_PREFIX = "backupset:";

  private final static byte[] EMPTY_VALUE = new byte[] {};
  
  // Safe delimiter in a string
  private final static String NULL = "\u0000";

  private BackupSystemTableHelper() {
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Creates Put operation for a given backup context object
   * @param context backup context
   * @return put operation
   * @throws IOException exception
   */
  static Put createPutForBackupContext(BackupInfo context) throws IOException {
    Put put = new Put(rowkey(BACKUP_INFO_PREFIX, context.getBackupId()));
    put.addColumn(BackupSystemTable.SESSIONS_FAMILY, "context".getBytes(), context.toByteArray());
    return put;
  }

  /**
   * Creates Get operation for a given backup id
   * @param backupId - backup's ID
   * @return get operation
   * @throws IOException exception
   */
  static Get createGetForBackupContext(String backupId) throws IOException {
    Get get = new Get(rowkey(BACKUP_INFO_PREFIX, backupId));
    get.addFamily(BackupSystemTable.SESSIONS_FAMILY);
    get.setMaxVersions(1);
    return get;
  }

  /**
   * Creates Delete operation for a given backup id
   * @param backupId - backup's ID
   * @return delete operation
   * @throws IOException exception
   */
  public static Delete createDeleteForBackupInfo(String backupId) {
    Delete del = new Delete(rowkey(BACKUP_INFO_PREFIX, backupId));
    del.addFamily(BackupSystemTable.SESSIONS_FAMILY);
    return del;
  }

  /**
   * Converts Result to BackupContext
   * @param res - HBase result
   * @return backup context instance
   * @throws IOException exception
   */
  static BackupInfo resultToBackupInfo(Result res) throws IOException {
    res.advance();
    Cell cell = res.current();
    return cellToBackupInfo(cell);
  }

  /**
   * Creates Get operation to retrieve start code from hbase:backup
   * @return get operation
   * @throws IOException exception
   */
  static Get createGetForStartCode(String rootPath) throws IOException {    
    Get get = new Get(rowkey(START_CODE_ROW, rootPath));
    get.addFamily(BackupSystemTable.META_FAMILY);
    get.setMaxVersions(1);
    return get;
  }

  /**
   * Creates Put operation to store start code to hbase:backup
   * @return put operation
   * @throws IOException exception
   */
  static Put createPutForStartCode(String startCode, String rootPath) {
    Put put = new Put(rowkey(START_CODE_ROW, rootPath));
    put.addColumn(BackupSystemTable.META_FAMILY, "startcode".getBytes(), startCode.getBytes());
    return put;
  }

  /**
   * Creates Get to retrieve incremental backup table set from hbase:backup
   * @return get operation
   * @throws IOException exception
   */
  static Get createGetForIncrBackupTableSet(String backupRoot) throws IOException {
    Get get = new Get(rowkey(INCR_BACKUP_SET, backupRoot));
    get.addFamily(BackupSystemTable.META_FAMILY);
    get.setMaxVersions(1);
    return get;
  }

  /**
   * Creates Put to store incremental backup table set
   * @param tables tables
   * @return put operation
   */
  static Put createPutForIncrBackupTableSet(Set<TableName> tables, String backupRoot) {
    Put put = new Put(rowkey(INCR_BACKUP_SET, backupRoot));
    for (TableName table : tables) {
      put.addColumn(BackupSystemTable.META_FAMILY, Bytes.toBytes(table.getNameAsString()),
        EMPTY_VALUE);
    }
    return put;
  }

  /**
   * Creates Scan operation to load backup history
   * @return scan operation
   */
  static Scan createScanForBackupHistory() {
    Scan scan = new Scan();
    byte[] startRow = BACKUP_INFO_PREFIX.getBytes();
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.SESSIONS_FAMILY);
    scan.setMaxVersions(1);
    return scan;
  }

  /**
   * Converts cell to backup context instance.
   * @param current - cell
   * @return backup context instance
   * @throws IOException exception
   */
  static BackupInfo cellToBackupInfo(Cell current) throws IOException {
    byte[] data = CellUtil.cloneValue(current);
    return BackupInfo.fromByteArray(data);
  }

  /**
   * Creates Put to write RS last roll log timestamp map
   * @param table - table
   * @param smap - map, containing RS:ts
   * @return put operation
   */
  static Put createPutForWriteRegionServerLogTimestamp(TableName table, byte[] smap, 
      String backupRoot) {    
    Put put = new Put(rowkey(TABLE_RS_LOG_MAP_PREFIX, backupRoot, NULL, table.getNameAsString()));
    put.addColumn(BackupSystemTable.META_FAMILY, "log-roll-map".getBytes(), smap);
    return put;
  }

  /**
   * Creates Scan to load table-> { RS -> ts} map of maps
   * @return scan operation
   */
  static Scan createScanForReadLogTimestampMap(String backupRoot) {
    Scan scan = new Scan();
    byte[] startRow = rowkey(TABLE_RS_LOG_MAP_PREFIX, backupRoot);
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.META_FAMILY);

    return scan;
  }

  /**
   * Get table name from rowkey
   * @param cloneRow rowkey
   * @return table name
   */
  static String getTableNameForReadLogTimestampMap(byte[] cloneRow) {
    String s = new String(cloneRow);
    int index = s.lastIndexOf(NULL); 
    return s.substring(index +1);
  }

  /**
   * Creates Put to store RS last log result
   * @param server - server name
   * @param timestamp - log roll result (timestamp)
   * @return put operation
   */
  static Put createPutForRegionServerLastLogRollResult(String server, 
      Long timestamp, String backupRoot ) {
    Put put = new Put(rowkey(RS_LOG_TS_PREFIX, backupRoot, NULL, server));
    put.addColumn(BackupSystemTable.META_FAMILY, "rs-log-ts".getBytes(), 
      timestamp.toString().getBytes());
    return put;
  }

  /**
   * Creates Scan operation to load last RS log roll results
   * @return scan operation
   */
  static Scan createScanForReadRegionServerLastLogRollResult(String backupRoot) {
    Scan scan = new Scan();
    byte[] startRow = rowkey(RS_LOG_TS_PREFIX, backupRoot);
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.META_FAMILY);
    scan.setMaxVersions(1);

    return scan;
  }

  /**
   * Get server's name from rowkey
   * @param row - rowkey
   * @return server's name
   */
  static String getServerNameForReadRegionServerLastLogRollResult(byte[] row) {
    String s = new String(row);
    int index = s.lastIndexOf(NULL);
    return s.substring(index +1);
  }

  /**
   * Creates put list for list of WAL files
   * @param files list of WAL file paths
   * @param backupId backup id
   * @return put list
   * @throws IOException exception
   */
  public static List<Put> createPutsForAddWALFiles(List<String> files, 
    String backupId, String backupRoot)
      throws IOException {

    List<Put> puts = new ArrayList<Put>();
    for (String file : files) {
      Put put = new Put(rowkey(WALS_PREFIX, BackupClientUtil.getUniqueWALFileNamePart(file)));
      put.addColumn(BackupSystemTable.META_FAMILY, "backupId".getBytes(), backupId.getBytes());
      put.addColumn(BackupSystemTable.META_FAMILY, "file".getBytes(), file.getBytes());
      put.addColumn(BackupSystemTable.META_FAMILY, "root".getBytes(), backupRoot.getBytes());
      puts.add(put);
    }
    return puts;
  }

  /**
   * Creates Scan operation to load WALs
   * TODO: support for backupRoot
   * @param backupRoot - path to backup destination 
   * @return scan operation
   */
  public static Scan createScanForGetWALs(String backupRoot) {
    Scan scan = new Scan();
    byte[] startRow = WALS_PREFIX.getBytes();
    byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addFamily(BackupSystemTable.META_FAMILY);
    return scan;
  }
  /**
   * Creates Get operation for a given wal file name
   * TODO: support for backup destination
   * @param file file
   * @return get operation
   * @throws IOException exception
   */
  public static Get createGetForCheckWALFile(String file) throws IOException {
    Get get = new Get(rowkey(WALS_PREFIX, BackupClientUtil.getUniqueWALFileNamePart(file)));
    // add backup root column
    get.addFamily(BackupSystemTable.META_FAMILY);
    return get;
  }

  
 /**
  * Creates Scan operation to load backup set list
  * @return scan operation
  */
 static Scan createScanForBackupSetList() {
   Scan scan = new Scan();
   byte[] startRow = SET_KEY_PREFIX.getBytes();
   byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
   stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
   scan.setStartRow(startRow);
   scan.setStopRow(stopRow);
   scan.addFamily(BackupSystemTable.META_FAMILY);
   return scan;
 }

 /**
  * Creates Get operation to load backup set content
  * @return get operation
  */
 static Get createGetForBackupSet(String name) {    
   Get get  = new Get(rowkey(SET_KEY_PREFIX, name));
   get.addFamily(BackupSystemTable.META_FAMILY);
   return get;
 }
 
 /**
  * Creates Delete operation to delete backup set content
  * @param name - backup set's name
  * @return delete operation
  */
 static Delete createDeleteForBackupSet(String name) {    
   Delete del  = new Delete(rowkey(SET_KEY_PREFIX, name));
   del.addFamily(BackupSystemTable.META_FAMILY);
   return del;
 }
 
 
 /**
  * Creates Put operation to update backup set content
  * @param name - backup set's name
  * @param tables - list of tables
  * @return put operation
  */
 static Put createPutForBackupSet(String name, String[] tables) {    
   Put put  = new Put(rowkey(SET_KEY_PREFIX, name));
   byte[] value = convertToByteArray(tables);
   put.addColumn(BackupSystemTable.META_FAMILY, "tables".getBytes(), value);
   return put;
 }
 
 private static byte[] convertToByteArray(String[] tables) {
   return StringUtils.join(tables, ",").getBytes();
 }

 
 /**
  * Converts cell to backup set list.
  * @param current - cell
  * @return backup set 
  * @throws IOException
  */
 static  String[] cellValueToBackupSet(Cell current) throws IOException {
   byte[] data = CellUtil.cloneValue(current);
   if( data != null && data.length > 0){
     return new String(data).split(",");
   } else{
     return new String[0];
   }
 }

 /**
  * Converts cell key to backup set name.
  * @param current - cell
  * @return backup set name
  * @throws IOException
  */
 static  String cellKeyToBackupSetName(Cell current) throws IOException {
   byte[] data = CellUtil.cloneRow(current);    
   return new String(data).substring(SET_KEY_PREFIX.length());    
 }
 
 static byte[] rowkey(String s, String ... other){
   StringBuilder sb = new StringBuilder(s);
   for(String ss: other){
     sb.append(ss);
   }
   return sb.toString().getBytes();   
 }
 
}
