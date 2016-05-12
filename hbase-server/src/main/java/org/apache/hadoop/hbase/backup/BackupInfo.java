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

package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.BackupInfo.Builder;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.TableBackupStatus;


/**
 * An object to encapsulate the information for each backup request
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupInfo implements Comparable<BackupInfo> {
  private static final Log LOG = LogFactory.getLog(BackupInfo.class);
  // backup status flag
  public static enum BackupState {
    WAITING, RUNNING, COMPLETE, FAILED, CANCELLED;
  }
  // backup phase    
  public static enum BackupPhase {
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
  
  // distributed job id
  private String jobId;
  
  // Number of parallel workers. -1 - system defined
  private int workers = -1;
 
  // Bandwidth per worker in MB per sec. -1 - unlimited
  private long bandwidth = -1;  
  
  public BackupInfo() {
  }

  public BackupInfo(String backupId, BackupType type, TableName[] tables, String targetRootDir) {
    backupStatusMap = new HashMap<TableName, BackupStatus>();

    this.backupId = backupId;
    this.type = type;
    this.targetRootDir = targetRootDir;
    if(LOG.isDebugEnabled()){
      LOG.debug("CreateBackupContext: " + tables.length+" "+tables[0] );
    }
    this.addTables(tables);

    if (type == BackupType.INCREMENTAL) {
      setHlogTargetDir(HBackupFileSystem.getLogBackupDir(targetRootDir, backupId));
    }

    this.startTs = 0;
    this.endTs = 0;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public int getWorkers() {
    return workers;
  }

  public void setWorkers(int workers) {
    this.workers = workers;
  }

  public long getBandwidth() {
    return bandwidth;
  }

  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  public void setBackupStatusMap(Map<TableName, BackupStatus> backupStatusMap) {
    this.backupStatusMap = backupStatusMap;
  }

  public HashMap<TableName, HashMap<String, Long>> getTableSetTimestampMap() {
    return tableSetTimestampMap;
  }
  
  public void setTableSetTimestampMap(HashMap<TableName, HashMap<String, Long>> tableSetTimestampMap) {
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

  public void setCancelled(boolean cancelled) {
    this.state = BackupState.CANCELLED;;
  }
  
  /**
   * Set progress (0-100%)
   * @param msg progress value
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

  public void setIncrBackupFileList(List<String> incrBackupFileList) {
    this.incrBackupFileList = incrBackupFileList;
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

  public BackupProtos.BackupInfo toProtosBackupInfo() {
    BackupProtos.BackupInfo.Builder builder = BackupProtos.BackupInfo.newBuilder();
    builder.setBackupId(getBackupId());
    setBackupStatusMap(builder);
    builder.setEndTs(getEndTs());
    if (getFailedMsg() != null) {
      builder.setFailedMessage(getFailedMsg());
    }
    if (getState() != null) {
      builder.setState(BackupProtos.BackupInfo.BackupState.valueOf(getState().name()));
    }
    if (getPhase() != null) {
      builder.setPhase(BackupProtos.BackupInfo.BackupPhase.valueOf(getPhase().name()));
    }

    builder.setProgress(getProgress());
    builder.setStartTs(getStartTs());
    builder.setTargetRootDir(getTargetRootDir());
    builder.setType(BackupProtos.BackupType.valueOf(getType().name()));
    builder.setWorkersNumber(workers);
    builder.setBandwidth(bandwidth);
    if (jobId != null) {
      builder.setJobId(jobId);
    }
    return builder.build();
  }

  public byte[] toByteArray() throws IOException {
    return toProtosBackupInfo().toByteArray();
  }

  private void setBackupStatusMap(Builder builder) {
    for (Entry<TableName, BackupStatus> entry: backupStatusMap.entrySet()) {
      builder.addTableBackupStatus(entry.getValue().toProto());
    }
  }

  public static BackupInfo fromByteArray(byte[] data) throws IOException {
    return fromProto(BackupProtos.BackupInfo.parseFrom(data));
  }
  
  public static BackupInfo fromStream(final InputStream stream) throws IOException {
    return fromProto(BackupProtos.BackupInfo.parseDelimitedFrom(stream));
  }

  public static BackupInfo fromProto(BackupProtos.BackupInfo proto) {
    BackupInfo context = new BackupInfo();
    context.setBackupId(proto.getBackupId());
    context.setBackupStatusMap(toMap(proto.getTableBackupStatusList()));
    context.setEndTs(proto.getEndTs());
    if (proto.hasFailedMessage()) {
      context.setFailedMsg(proto.getFailedMessage());
    }
    if (proto.hasState()) {
      context.setState(BackupInfo.BackupState.valueOf(proto.getState().name()));
    }

    context.setHlogTargetDir(HBackupFileSystem.getLogBackupDir(proto.getTargetRootDir(),
      proto.getBackupId()));

    if (proto.hasPhase()) {
      context.setPhase(BackupPhase.valueOf(proto.getPhase().name()));
    }
    if (proto.hasProgress()) {
      context.setProgress(proto.getProgress());
    }
    context.setStartTs(proto.getStartTs());
    context.setTargetRootDir(proto.getTargetRootDir());
    context.setType(BackupType.valueOf(proto.getType().name()));
    context.setWorkers(proto.getWorkersNumber());
    context.setBandwidth(proto.getBandwidth());
    if (proto.hasJobId()) {
      context.setJobId(proto.getJobId());
    }
    return context;
  }

  private static Map<TableName, BackupStatus> toMap(List<TableBackupStatus> list) {
    HashMap<TableName, BackupStatus> map = new HashMap<>();
    for (TableBackupStatus tbs : list){
      map.put(ProtobufUtil.toTableName(tbs.getTable()), BackupStatus.convert(tbs));
    }
    return map;
  }

  public String getShortDescription() {
    StringBuilder sb = new StringBuilder();
    sb.append("ID             : " + backupId).append("\n");
    sb.append("Type           : " + getType()).append("\n");
    sb.append("Tables         : " + getTableListAsString()).append("\n");
    sb.append("State          : " + getState()).append("\n");
    Date date = null;
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(getStartTs());
    date = cal.getTime();
    sb.append("Start time     : " + date).append("\n");
    if (state == BackupState.FAILED) {
      sb.append("Failed message : " + getFailedMsg()).append("\n");
    } else if (state == BackupState.RUNNING) {
      sb.append("Phase          : " + getPhase()).append("\n");
    } else if (state == BackupState.COMPLETE) {
      cal = Calendar.getInstance();
      cal.setTimeInMillis(getEndTs());
      date = cal.getTime();
      sb.append("End time       : " + date).append("\n");
    }
    sb.append("Progress       : " + getProgress()).append("\n");
    return sb.toString();
  }

  public String getStatusAndProgressAsString() {
    StringBuilder sb = new StringBuilder();
    sb.append("id: ").append(getBackupId()).append(" state: ").append(getState())
        .append(" progress: ").append(getProgress());
    return sb.toString();
  }
  
  public String getTableListAsString() {
    return StringUtils.join(backupStatusMap.keySet(), ";");
  }

  @Override
  public int compareTo(BackupInfo o) {
      Long thisTS =
          new Long(this.getBackupId().substring(this.getBackupId().lastIndexOf("_") + 1));
      Long otherTS =
          new Long(o.getBackupId().substring(o.getBackupId().lastIndexOf("_") + 1));
      return thisTS.compareTo(otherTS);
  }
   
  
}
