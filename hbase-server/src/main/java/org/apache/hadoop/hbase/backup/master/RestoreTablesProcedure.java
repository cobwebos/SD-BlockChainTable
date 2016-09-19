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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.util.RestoreServerUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreTablesState;

@InterfaceAudience.Private
public class RestoreTablesProcedure
    extends StateMachineProcedure<MasterProcedureEnv, RestoreTablesState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(RestoreTablesProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);
  private Configuration conf;
  private String backupId;
  private List<TableName> sTableList;
  private List<TableName> tTableList;
  private String targetRootDir;
  private boolean isOverwrite;

  public RestoreTablesProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public RestoreTablesProcedure(final MasterProcedureEnv env,
      final String targetRootDir, String backupId, List<TableName> sTableList,
      List<TableName> tTableList, boolean isOverwrite) throws IOException {
    this.targetRootDir = targetRootDir;
    this.backupId = backupId;
    this.sTableList = sTableList;
    this.tTableList = tTableList;
    if (tTableList == null || tTableList.isEmpty()) {
      this.tTableList = sTableList;
    }
    this.isOverwrite = isOverwrite;
    this.setOwner(env.getRequestUser().getUGI().getShortUserName());
  }

  @Override
  public byte[] getResult() {
    return null;
  }

  /**
   * Validate target Tables
   * @param conn connection
   * @param mgr table state manager
   * @param tTableArray: target tables
   * @param isOverwrite overwrite existing table
   * @throws IOException exception
   */
  private void checkTargetTables(Connection conn, TableStateManager mgr, TableName[] tTableArray,
      boolean isOverwrite)
      throws IOException {
    ArrayList<TableName> existTableList = new ArrayList<>();
    ArrayList<TableName> disabledTableList = new ArrayList<>();

    // check if the tables already exist
    for (TableName tableName : tTableArray) {
      if (MetaTableAccessor.tableExists(conn, tableName)) {
        existTableList.add(tableName);
        if (mgr.isTableState(tableName, TableState.State.DISABLED, TableState.State.DISABLING)) {
          disabledTableList.add(tableName);
        }
      } else {
        LOG.info("HBase table " + tableName
            + " does not exist. It will be created during restore process");
      }
    }

    if (existTableList.size() > 0) {
      if (!isOverwrite) {
        LOG.error("Existing table (" + existTableList + ") found in the restore target, please add "
          + "\"-overwrite\" option in the command if you mean to restore to these existing tables");
        throw new IOException("Existing table found in target while no \"-overwrite\" "
            + "option found");
      } else {
        if (disabledTableList.size() > 0) {
          LOG.error("Found offline table in the restore target, "
              + "please enable them before restore with \"-overwrite\" option");
          LOG.info("Offline table list in restore target: " + disabledTableList);
          throw new IOException(
              "Found offline table in the target when restore with \"-overwrite\" option");
        }
      }
    }
  }

  /**
   * Restore operation handle each backupImage in array
   * @param svc: master services
   * @param images: array BackupImage
   * @param sTable: table to be restored
   * @param tTable: table to be restored to
   * @param truncateIfExists: truncate table
   * @throws IOException exception
   */

  private void restoreImages(MasterServices svc, BackupImage[] images, TableName sTable, TableName tTable,
      boolean truncateIfExists) throws IOException {

    // First image MUST be image of a FULL backup
    BackupImage image = images[0];
    String rootDir = image.getRootDir();
    String backupId = image.getBackupId();
    Path backupRoot = new Path(rootDir);
    RestoreServerUtil restoreTool = new RestoreServerUtil(conf, backupRoot, backupId);
    Path tableBackupPath = HBackupFileSystem.getTableBackupPath(sTable, backupRoot, backupId);
    String lastIncrBackupId = images.length == 1 ? null : images[images.length - 1].getBackupId();
    // We need hFS only for full restore (see the code)
    BackupManifest manifest = HBackupFileSystem.getManifest(sTable, conf, backupRoot, backupId);
    if (manifest.getType() == BackupType.FULL) {
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from full"
          + " backup image " + tableBackupPath.toString());
      restoreTool.fullRestoreTable(svc, tableBackupPath, sTable, tTable, truncateIfExists,
        lastIncrBackupId);
    } else { // incremental Backup
      throw new IOException("Unexpected backup type " + image.getType());
    }

    if (images.length == 1) {
      // full backup restore done
      return;
    }

    List<Path> dirList = new ArrayList<Path>();
    // add full backup path
    // full backup path comes first
    for (int i = 1; i < images.length; i++) {
      BackupImage im = images[i];
      String logBackupDir = HBackupFileSystem.getLogBackupDir(im.getRootDir(), im.getBackupId());
      dirList.add(new Path(logBackupDir));
    }

    String dirs = StringUtils.join(dirList, ",");
    LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from log dirs: " + dirs);
    Path[] paths = new Path[dirList.size()];
    dirList.toArray(paths);
    restoreTool.incrementalRestoreTable(svc, tableBackupPath, paths, new TableName[] { sTable },
      new TableName[] { tTable }, lastIncrBackupId);
    LOG.info(sTable + " has been successfully restored to " + tTable);

  }

  /**
   * Restore operation. Stage 2: resolved Backup Image dependency
   * @param svc: master services
   * @param backupManifestMap : tableName,  Manifest
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @return set of BackupImages restored
   * @throws IOException exception
   */
  private void restore(MasterServices svc, HashMap<TableName, BackupManifest> backupManifestMap,
      TableName[] sTableArray, TableName[] tTableArray, boolean isOverwrite) throws IOException {
    TreeSet<BackupImage> restoreImageSet = new TreeSet<BackupImage>();
    boolean truncateIfExists = isOverwrite;
    try {
      for (int i = 0; i < sTableArray.length; i++) {
        TableName table = sTableArray[i];
        BackupManifest manifest = backupManifestMap.get(table);
        // Get the image list of this backup for restore in time order from old
        // to new.
        List<BackupImage> list = new ArrayList<BackupImage>();
        list.add(manifest.getBackupImage());
        TreeSet<BackupImage> set = new TreeSet<BackupImage>(list);
        List<BackupImage> depList = manifest.getDependentListByTable(table);
        set.addAll(depList);
        BackupImage[] arr = new BackupImage[set.size()];
        set.toArray(arr);
        restoreImages(svc, arr, table, tTableArray[i], truncateIfExists);
        restoreImageSet.addAll(list);
        if (restoreImageSet != null && !restoreImageSet.isEmpty()) {
          LOG.info("Restore includes the following image(s):");
          for (BackupImage image : restoreImageSet) {
            LOG.info("Backup: "
                + image.getBackupId()
                + " "
                + HBackupFileSystem.getTableBackupDir(image.getRootDir(), image.getBackupId(),
                  table));
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed", e);
      throw new IOException(e);
    }
    LOG.debug("restoreStage finished");
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final RestoreTablesState state)
      throws InterruptedException {
    if (conf == null) {
      conf = env.getMasterConfiguration();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    TableName[] tTableArray = tTableList.toArray(new TableName[tTableList.size()]);
    try {
      switch (state) {
        case VALIDATION:

          // check the target tables
          checkTargetTables(env.getMasterServices().getConnection(),
              env.getMasterServices().getTableStateManager(), tTableArray, isOverwrite);

          setNextState(RestoreTablesState.RESTORE_IMAGES);
          break;
        case RESTORE_IMAGES:
          TableName[] sTableArray = sTableList.toArray(new TableName[sTableList.size()]);
          HashMap<TableName, BackupManifest> backupManifestMap = new HashMap<>();
          // check and load backup image manifest for the tables
          Path rootPath = new Path(targetRootDir);
          HBackupFileSystem.checkImageManifestExist(backupManifestMap, sTableArray, conf, rootPath,
            backupId);
          restore(env.getMasterServices(), backupManifestMap, sTableArray, tTableArray, isOverwrite);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      setFailure("restore-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final RestoreTablesState state)
      throws IOException {
  }

  @Override
  protected RestoreTablesState getState(final int stateId) {
    return RestoreTablesState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final RestoreTablesState state) {
    return state.getNumber();
  }

  @Override
  protected RestoreTablesState getInitialState() {
    return RestoreTablesState.VALIDATION;
  }

  @Override
  protected void setNextState(final RestoreTablesState state) {
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
    sb.append(" isOverwrite= ");
    sb.append(isOverwrite);
    sb.append(" backupId= ");
    sb.append(backupId);
    sb.append(")");
  }

  MasterProtos.RestoreTablesRequest toRestoreTables() {
    MasterProtos.RestoreTablesRequest.Builder bldr = MasterProtos.RestoreTablesRequest.newBuilder();
    bldr.setOverwrite(isOverwrite).setBackupId(backupId);
    bldr.setBackupRootDir(targetRootDir);
    for (TableName table : sTableList) {
      bldr.addTables(ProtobufUtil.toProtoTableName(table));
    }
    for (TableName table : tTableList) {
      bldr.addTargetTables(ProtobufUtil.toProtoTableName(table));
    }
    return bldr.build();
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProtos.RestoreTablesRequest restoreTables = toRestoreTables();
    restoreTables.writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProtos.RestoreTablesRequest proto =
        MasterProtos.RestoreTablesRequest.parseDelimitedFrom(stream);
    backupId = proto.getBackupId();
    targetRootDir = proto.getBackupRootDir();
    isOverwrite = proto.getOverwrite();
    sTableList = new ArrayList<>(proto.getTablesList().size());
    for (HBaseProtos.TableName table : proto.getTablesList()) {
      sTableList.add(ProtobufUtil.toTableName(table));
    }
    tTableList = new ArrayList<>(proto.getTargetTablesList().size());
    for (HBaseProtos.TableName table : proto.getTargetTablesList()) {
      tTableList.add(ProtobufUtil.toTableName(table));
    }
  }

  @Override
  public TableName getTableName() {
    return TableName.BACKUP_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.RESTORE;
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
