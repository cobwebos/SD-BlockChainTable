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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.BackupContext.Builder;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.TableBackupStatus;

/**
 * An object to encapsulate the information for each backup request
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupContext {

  public Map<TableName, BackupStatus> getBackupStatusMap() {
    return backupStatusMap;
  }

  public void setBackupStatusMap(Map<TableName, BackupStatus> backupStatusMap) {
    this.backupStatusMap = backupStatusMap;
  }

  public HashMap<TableName, HashMap<String, Long>> getTableSetTimestampMap() {
    return tableSetTimestampMap;
  }

  public void setTableSetTimestampMap(
      HashMap<TableName, HashMap<String, Long>> tableSetTimestampMap) {
    this.tableSetTimestampMap = tableSetTimestampMap;
  }

  public String getHlogTargetDir() {
    return hlogTargetDir;
  }

  public void setType(BackupType type) {
    this.type = type;
  }

  public void setTargetRootDir(String targetRootDir) {
    this.targetRootDir = targetRootDir;
  }

  public void setTotalBytesCopied(long totalBytesCopied) {
    this.totalBytesCopied = totalBytesCopied;
  }

  // backup status flag
  public static enum BackupState {
    RUNNING, COMPLETE, FAILED, CANCELLED;
  }

  public void setCancelled(boolean cancelled) {
    this.state = BackupState.CANCELLED;;
  }

  // backup phase
  // for overall backup (for table list, some table may go online, while some may go offline)
  protected static enum BackupPhase {
    SNAPSHOTCOPY, INCREMENTAL_COPY, STORE_MANIFEST;
  }

  // backup id: a timestamp when we request the backup
  private String backupId;

  // backup type, full or incremental
  private BackupType type;

  // target root directory for storing the backup files
  private String targetRootDir;

  // overall backup state
  private BackupState state;

  // overall backup phase
  private BackupPhase phase;

  // overall backup failure message
  private String failedMsg;

  // backup status map for all tables
  private Map<TableName, BackupStatus> backupStatusMap;

  // actual start timestamp of the backup process
  private long startTs;

  // actual end timestamp of the backup process, could be fail or complete
  private long endTs;

  // the total bytes of incremental logs copied
  private long totalBytesCopied;

  // for incremental backup, the location of the backed-up hlogs
  private String hlogTargetDir = null;

  // incremental backup file list
  transient private List<String> incrBackupFileList;

  // new region server log timestamps for table set after distributed log roll
  // key - table name, value - map of RegionServer hostname -> last log rolled timestamp
  transient private HashMap<TableName, HashMap<String, Long>> tableSetTimestampMap;

  // backup progress in %% (0-100)

  private int progress;

  public BackupContext() {
  }

  public BackupContext(String backupId, BackupType type, TableName[] tables, String targetRootDir) {
    backupStatusMap = new HashMap<TableName, BackupStatus>();

    this.backupId = backupId;
    this.type = type;
    this.targetRootDir = targetRootDir;

    this.addTables(tables);

    if (type == BackupType.INCREMENTAL) {
      setHlogTargetDir(HBackupFileSystem.getLogBackupDir(targetRootDir, backupId));
    }

    this.startTs = 0;
    this.endTs = 0;
  }

  /**
   * Set progress string
   * @param msg progress message
   */

  public void setProgress(int p) {
    this.progress = p;
  }

  /**
   * Get current progress
   */
  public int getProgress() {
    return progress;
  }


  /**
   * Has been marked as cancelled or not.
   * @return True if marked as cancelled
   */
  public boolean isCancelled() {
    return this.state == BackupState.CANCELLED;
  }

  public String getBackupId() {
    return backupId;
  }

  public void setBackupId(String backupId) {
    this.backupId = backupId;
  }

  public BackupStatus getBackupStatus(TableName table) {
    return this.backupStatusMap.get(table);
  }

  public String getFailedMsg() {
    return failedMsg;
  }

  public void setFailedMsg(String failedMsg) {
    this.failedMsg = failedMsg;
  }

  public long getStartTs() {
    return startTs;
  }

  public void setStartTs(long startTs) {
    this.startTs = startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public void setEndTs(long endTs) {
    this.endTs = endTs;
  }

  public long getTotalBytesCopied() {
    return totalBytesCopied;
  }

  public BackupState getState() {
    return state;
  }

  public void setState(BackupState flag) {
    this.state = flag;
  }

  public BackupPhase getPhase() {
    return phase;
  }

  public void setPhase(BackupPhase phase) {
    this.phase = phase;
  }

  public BackupType getType() {
    return type;
  }

  public void setSnapshotName(TableName table, String snapshotName) {
    this.backupStatusMap.get(table).setSnapshotName(snapshotName);
  }

  public String getSnapshotName(TableName table) {
    return this.backupStatusMap.get(table).getSnapshotName();
  }

  public List<String> getSnapshotNames() {
    List<String> snapshotNames = new ArrayList<String>();
    for (BackupStatus backupStatus : this.backupStatusMap.values()) {
      snapshotNames.add(backupStatus.getSnapshotName());
    }
    return snapshotNames;
  }

  public Set<TableName> getTables() {
    return this.backupStatusMap.keySet();
  }

  public List<TableName> getTableNames() {
    return new ArrayList<TableName>(backupStatusMap.keySet());
  }

  public void addTables(TableName[] tables) {
    for (TableName table : tables) {
      BackupStatus backupStatus = new BackupStatus(table, this.targetRootDir, this.backupId);
      this.backupStatusMap.put(table, backupStatus);
    }
  }

  public String getTargetRootDir() {
    return targetRootDir;
  }

  public void setHlogTargetDir(String hlogTagetDir) {
    this.hlogTargetDir = hlogTagetDir;
  }

  public String getHLogTargetDir() {
    return hlogTargetDir;
  }

  public List<String> getIncrBackupFileList() {
    return incrBackupFileList;
  }

  public List<String> setIncrBackupFileList(List<String> incrBackupFileList) {
    this.incrBackupFileList = incrBackupFileList;
    return this.incrBackupFileList;
  }

  /**
   * Set the new region server log timestamps after distributed log roll
   * @param newTableSetTimestampMap table timestamp map
   */
  public void setIncrTimestampMap(HashMap<TableName,
      HashMap<String, Long>> newTableSetTimestampMap) {
    this.tableSetTimestampMap = newTableSetTimestampMap;
  }

  /**
   * Get new region server log timestamps after distributed log roll
   * @return new region server log timestamps
   */
  public HashMap<TableName, HashMap<String, Long>> getIncrTimestampMap() {
    return this.tableSetTimestampMap;
  }

  public TableName getTableBySnapshot(String snapshotName) {
    for (Entry<TableName, BackupStatus> entry : this.backupStatusMap.entrySet()) {
      if (snapshotName.equals(entry.getValue().getSnapshotName())) {
        return entry.getKey();
      }
    }
    return null;
  }

  BackupProtos.BackupContext toBackupContext() {
    BackupProtos.BackupContext.Builder builder =
        BackupProtos.BackupContext.newBuilder();
    builder.setBackupId(getBackupId());
    setBackupStatusMap(builder);
    builder.setEndTs(getEndTs());
    if(getFailedMsg() != null){
      builder.setFailedMessage(getFailedMsg());
    }
    if(getState() != null){
      builder.setState(BackupProtos.BackupContext.BackupState.valueOf(getState().name()));
    }
    if(getPhase() != null){
      builder.setPhase(BackupProtos.BackupContext.BackupPhase.valueOf(getPhase().name()));
    }
    if(getHLogTargetDir() != null){
      builder.setHlogTargetDir(getHLogTargetDir());
    }

    builder.setProgress(getProgress());
    builder.setStartTs(getStartTs());
    builder.setTargetRootDir(getTargetRootDir());
    builder.setTotalBytesCopied(getTotalBytesCopied());
    builder.setType(BackupProtos.BackupType.valueOf(getType().name()));
    return builder.build();
  }

  public byte[] toByteArray() throws IOException {
    return toBackupContext().toByteArray();
  }

  private void setBackupStatusMap(Builder builder) {
    for (Entry<TableName, BackupStatus> entry: backupStatusMap.entrySet()) {
      builder.addTableBackupStatus(entry.getValue().toProto());
    }
  }

  public static BackupContext fromByteArray(byte[] data) throws IOException {
    return fromProto(BackupProtos.BackupContext.parseFrom(data));
  }
  
  public static BackupContext fromStream(final InputStream stream) throws IOException {
    return fromProto(BackupProtos.BackupContext.parseDelimitedFrom(stream));
  }

  static BackupContext fromProto(BackupProtos.BackupContext proto) {
    BackupContext context = new BackupContext();
    context.setBackupId(proto.getBackupId());
    context.setBackupStatusMap(toMap(proto.getTableBackupStatusList()));
    context.setEndTs(proto.getEndTs());
    if(proto.hasFailedMessage()) {
      context.setFailedMsg(proto.getFailedMessage());
    }
    if(proto.hasState()) {
      context.setState(BackupContext.BackupState.valueOf(proto.getState().name()));
    }
    if(proto.hasHlogTargetDir()) {
      context.setHlogTargetDir(proto.getHlogTargetDir());
    }
    if(proto.hasPhase()) {
      context.setPhase(BackupPhase.valueOf(proto.getPhase().name()));
    }
    if(proto.hasProgress()) {
      context.setProgress(proto.getProgress());
    }
    context.setStartTs(proto.getStartTs());
    context.setTargetRootDir(proto.getTargetRootDir());
    context.setTotalBytesCopied(proto.getTotalBytesCopied());
    context.setType(BackupType.valueOf(proto.getType().name()));
    return context;
  }

  private static Map<TableName, BackupStatus> toMap(List<TableBackupStatus> list) {
    HashMap<TableName, BackupStatus> map = new HashMap<>();
    for (TableBackupStatus tbs : list){
      map.put(ProtobufUtil.toTableName(tbs.getTable()), BackupStatus.convert(tbs));
    }
    return map;
  }

}
