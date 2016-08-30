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

package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyService;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreServerFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.BackupCopyService.Type;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.IncrementalBackupManager;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.backup.util.BackupServerUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.IncrementalTableBackupState;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos.ServerTimestamp;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
public class IncrementalTableBackupProcedure
    extends StateMachineProcedure<MasterProcedureEnv, IncrementalTableBackupState> 
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(IncrementalTableBackupProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);
  private Configuration conf;
  private String backupId;
  private List<TableName> tableList;
  private String targetRootDir;
  HashMap<String, Long> newTimestamps = null;

  private BackupManager backupManager;
  private BackupInfo backupContext;

  public IncrementalTableBackupProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public IncrementalTableBackupProcedure(final MasterProcedureEnv env,
      final String backupId,
      List<TableName> tableList, String targetRootDir, final int workers,
      final long bandwidth) throws IOException {
    backupManager = new BackupManager(env.getMasterConfiguration());
    this.backupId = backupId;
    this.tableList = tableList;
    this.targetRootDir = targetRootDir;
    backupContext = backupManager.createBackupContext(backupId, 
      BackupType.INCREMENTAL, tableList, targetRootDir, workers, (int)bandwidth);
    this.setOwner(env.getRequestUser().getUGI().getShortUserName());
  }

  @Override
  public byte[] getResult() {
    return backupId.getBytes();
  }

  private List<String> filterMissingFiles(List<String> incrBackupFileList) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    List<String> list = new ArrayList<String>();
    for (String file : incrBackupFileList) {
      if (fs.exists(new Path(file))) {
        list.add(file);
      } else {
        LOG.warn("Can't find file: " + file);
      }
    }
    return list;
  }
  
  private List<String> getMissingFiles(List<String> incrBackupFileList) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    List<String> list = new ArrayList<String>();
    for (String file : incrBackupFileList) {
      if (!fs.exists(new Path(file))) {
        list.add(file);
      }
    }
    return list;
    
  }

  /**
   * Do incremental copy.
   * @param backupContext backup context
   */
  private void incrementalCopy(BackupInfo backupContext) throws Exception {

    LOG.info("Incremental copy is starting.");
    // set overall backup phase: incremental_copy
    backupContext.setPhase(BackupPhase.INCREMENTAL_COPY);
    // get incremental backup file list and prepare parms for DistCp
    List<String> incrBackupFileList = backupContext.getIncrBackupFileList();
    // filter missing files out (they have been copied by previous backups)
    incrBackupFileList = filterMissingFiles(incrBackupFileList);
    String[] strArr = incrBackupFileList.toArray(new String[incrBackupFileList.size() + 1]);
    strArr[strArr.length - 1] = backupContext.getHLogTargetDir();

    BackupCopyService copyService = BackupRestoreServerFactory.getBackupCopyService(conf);
    int counter = 0;
    int MAX_ITERAIONS = 2;
    while (counter++ < MAX_ITERAIONS) { 
      // We run DistCp maximum 2 times
      // If it fails on a second time, we throw Exception
      int res = copyService.copy(backupContext, backupManager, conf,
        BackupCopyService.Type.INCREMENTAL, strArr);

      if (res != 0) {
        LOG.error("Copy incremental log files failed with return code: " + res + ".");
        throw new IOException("Failed of Hadoop Distributed Copy from "+
            StringUtils.join(incrBackupFileList, ",") +" to "
          + backupContext.getHLogTargetDir());
      }
      List<String> missingFiles = getMissingFiles(incrBackupFileList);

      if(missingFiles.isEmpty()) {
        break;
      } else {
        // Repeat DistCp, some files have been moved from WALs to oldWALs during previous run
        // update backupContext and strAttr
        if(counter == MAX_ITERAIONS){
          String msg = "DistCp could not finish the following files: " +
           StringUtils.join(missingFiles, ",");
          LOG.error(msg);
          throw new IOException(msg);
        }
        List<String> converted = convertFilesFromWALtoOldWAL(missingFiles);
        incrBackupFileList.removeAll(missingFiles);
        incrBackupFileList.addAll(converted);
        backupContext.setIncrBackupFileList(incrBackupFileList);
        
        // Run DistCp only for missing files (which have been moved from WALs to oldWALs 
        // during previous run)
        strArr = converted.toArray(new String[converted.size() + 1]);
        strArr[strArr.length - 1] = backupContext.getHLogTargetDir();
      }
    }
    
    
    LOG.info("Incremental copy from " + StringUtils.join(incrBackupFileList, ",") + " to "
        + backupContext.getHLogTargetDir() + " finished.");
  }


  private List<String> convertFilesFromWALtoOldWAL(List<String> missingFiles) throws IOException {
    List<String> list = new ArrayList<String>();
    for(String path: missingFiles){
      if(path.indexOf(Path.SEPARATOR + HConstants.HREGION_LOGDIR_NAME) < 0) {
        LOG.error("Copy incremental log files failed, file is missing : " + path);
        throw new IOException("Failed of Hadoop Distributed Copy to "
          + backupContext.getHLogTargetDir()+", file is missing "+ path);
      }
      list.add(path.replace(Path.SEPARATOR + HConstants.HREGION_LOGDIR_NAME, 
        Path.SEPARATOR + HConstants.HREGION_OLDLOGDIR_NAME));
    }
    return list;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env,
      final IncrementalTableBackupState state)
      throws InterruptedException {
    if (conf == null) {
      conf = env.getMasterConfiguration();
    }
    if (backupManager == null) {
      try {
        backupManager = new BackupManager(env.getMasterConfiguration());
      } catch (IOException ioe) {
        setFailure("incremental backup", ioe);
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case PREPARE_INCREMENTAL:
          FullTableBackupProcedure.beginBackup(backupManager, backupContext);
          LOG.debug("For incremental backup, current table set is "
              + backupManager.getIncrementalBackupTableSet());
          try {
            IncrementalBackupManager incrBackupManager =new IncrementalBackupManager(backupManager);

            newTimestamps = incrBackupManager.getIncrBackupLogFileList(backupContext);
          } catch (Exception e) {
            setFailure("Failure in incremental-backup: preparation phase " + backupId, e);
            // fail the overall backup and return
            FullTableBackupProcedure.failBackup(env, backupContext, backupManager, e,
              "Unexpected Exception : ", BackupType.INCREMENTAL, conf);
          }

          setNextState(IncrementalTableBackupState.INCREMENTAL_COPY);
          break;
        case INCREMENTAL_COPY:
          try {
            // copy out the table and region info files for each table
            BackupServerUtil.copyTableRegionInfo(backupContext, conf);
            incrementalCopy(backupContext);
            // Save list of WAL files copied
            backupManager.recordWALFiles(backupContext.getIncrBackupFileList());
          } catch (Exception e) {
            String msg = "Unexpected exception in incremental-backup: incremental copy " + backupId;
            setFailure(msg, e);
            // fail the overall backup and return
            FullTableBackupProcedure.failBackup(env, backupContext, backupManager, e,
              msg, BackupType.INCREMENTAL, conf);
          }
          setNextState(IncrementalTableBackupState.INCR_BACKUP_COMPLETE);
          break;
        case INCR_BACKUP_COMPLETE:
          // set overall backup status: complete. Here we make sure to complete the backup.
          // After this checkpoint, even if entering cancel process, will let the backup finished
          backupContext.setState(BackupState.COMPLETE);
          // Set the previousTimestampMap which is before this current log roll to the manifest.
          HashMap<TableName, HashMap<String, Long>> previousTimestampMap =
              backupManager.readLogTimestampMap();
          backupContext.setIncrTimestampMap(previousTimestampMap);

          // The table list in backupContext is good for both full backup and incremental backup.
          // For incremental backup, it contains the incremental backup table set.
          backupManager.writeRegionServerLogTimestamp(backupContext.getTables(), newTimestamps);

          HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
              backupManager.readLogTimestampMap();

          Long newStartCode = BackupClientUtil
              .getMinValue(BackupServerUtil.getRSLogTimestampMins(newTableSetTimestampMap));
          backupManager.writeBackupStartCode(newStartCode);
          // backup complete
          FullTableBackupProcedure.completeBackup(env, backupContext, backupManager,
            BackupType.INCREMENTAL, conf);
          return Flow.NO_MORE_STATE;

        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      setFailure("snapshot-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env,
      final IncrementalTableBackupState state) throws IOException {
    // clean up the uncompleted data at target directory if the ongoing backup has already entered
    // the copy phase
    // For incremental backup, DistCp logs will be cleaned with the targetDir.
    FullTableBackupProcedure.cleanupTargetDir(backupContext, conf);
  }

  @Override
  protected IncrementalTableBackupState getState(final int stateId) {
    return IncrementalTableBackupState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final IncrementalTableBackupState state) {
    return state.getNumber();
  }

  @Override
  protected IncrementalTableBackupState getInitialState() {
    return IncrementalTableBackupState.PREPARE_INCREMENTAL;
  }

  @Override
  protected void setNextState(final IncrementalTableBackupState state) {
    if (aborted.get()) {
      setAbortFailure("snapshot-table", "abort requested");
    } else {
      super.setNextState(state);
    }
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    aborted.set(true);
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (targetRootDir=");
    sb.append(targetRootDir);
    sb.append("; backupId=").append(backupId);
    sb.append("; tables=");
    int len = tableList.size();
    for (int i = 0; i < len-1; i++) {
      sb.append(tableList.get(i)).append(",");
    }
    sb.append(tableList.get(len-1));
    sb.append(")");
  }

  BackupProtos.BackupProcContext toBackupContext() {
    BackupProtos.BackupProcContext.Builder ctxBuilder = BackupProtos.BackupProcContext.newBuilder();
    ctxBuilder.setCtx(backupContext.toProtosBackupInfo());
    if (newTimestamps != null && !newTimestamps.isEmpty()) {
      BackupProtos.ServerTimestamp.Builder tsBuilder = ServerTimestamp.newBuilder();
      for (Entry<String, Long> entry : newTimestamps.entrySet()) {
        tsBuilder.clear().setServer(entry.getKey()).setTimestamp(entry.getValue());
        ctxBuilder.addServerTimestamp(tsBuilder.build());
      }
    }
    return ctxBuilder.build();
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    BackupProtos.BackupProcContext backupProcCtx = toBackupContext();
    backupProcCtx.writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    BackupProtos.BackupProcContext proto =BackupProtos.BackupProcContext.parseDelimitedFrom(stream);
    backupContext = BackupInfo.fromProto(proto.getCtx());
    backupId = backupContext.getBackupId();
    targetRootDir = backupContext.getTargetRootDir();
    tableList = backupContext.getTableNames();
    List<ServerTimestamp> svrTimestamps = proto.getServerTimestampList();
    if (svrTimestamps != null && !svrTimestamps.isEmpty()) {
      newTimestamps = new HashMap<>();
      for (ServerTimestamp ts : svrTimestamps) {
        newTimestamps.put(ts.getServer(), ts.getTimestamp());
      }
    }
  }

  @Override
  public TableName getTableName() {
    return TableName.BACKUP_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.BACKUP;
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) {
      return false;
    }
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(this, TableName.BACKUP_TABLE_NAME);
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(this, TableName.BACKUP_TABLE_NAME);
  }
}
