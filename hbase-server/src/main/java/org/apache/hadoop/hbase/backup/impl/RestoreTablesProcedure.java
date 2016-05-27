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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.util.RestoreServerUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
  private List<TableName> tableList;
  private List<TableName> tTableList;
  private String targetRootDir;
  private boolean autoRestore;
  private boolean isOverwrite;

  public RestoreTablesProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public RestoreTablesProcedure(final MasterProcedureEnv env,
      final String targetRootDir, String backupId, boolean autoRestore, List<TableName> sTableList,
      List<TableName> tTableList, boolean isOverwrite) throws IOException {
    this.targetRootDir = targetRootDir;
    this.backupId = backupId;
    this.tableList = sTableList;
    this.tTableList = tTableList;
    if (tTableList == null || tTableList.isEmpty()) {
      this.tTableList = tableList;
    }
    this.autoRestore = autoRestore;
    this.isOverwrite = isOverwrite;
  }

  @Override
  public byte[] getResult() {
    return null;
  }

  /**
   * Validate target Tables
   * @param tTableArray: target tables
   * @param isOverwrite overwrite existing table
   * @throws IOException exception
   */
  private  void checkTargetTables(TableName[] tTableArray, boolean isOverwrite)
      throws IOException {
    ArrayList<TableName> existTableList = new ArrayList<>();
    ArrayList<TableName> disabledTableList = new ArrayList<>();

    // check if the tables already exist
    try(Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin()) {
      for (TableName tableName : tTableArray) {
        if (admin.tableExists(tableName)) {
          existTableList.add(tableName);
          if (admin.isTableDisabled(tableName)) {
            disabledTableList.add(tableName);
          }
        } else {
          LOG.info("HBase table " + tableName
              + " does not exist. It will be created during restore process");
        }
      }
    }

    if (existTableList.size() > 0) {
      if (!isOverwrite) {
        IOException ioe = new IOException("Existing table found in target while no \"-overwrite\" "
            + "option found");
        setFailure("Existing table found in the restore target, please add \"-overwrite\" "
            + "option in the command if you mean to restore to these existing tables", ioe);
        LOG.info("Existing table list in restore target: " + existTableList);
        throw ioe;
      } else {
        if (disabledTableList.size() > 0) {
          IOException ioe = new IOException(
              "Found offline table in the target when restore with \"-overwrite\" option");
          setFailure("Found offline table in the restore target, "
              + "please enable them before restore with \"-overwrite\" option", ioe);
          LOG.info("Offline table list in restore target: " + disabledTableList);
          throw ioe;
        }
      }
    }
  }

  /**
   * Restore operation handle each backupImage in iterator
   * @param it: backupImage iterator - ascending
   * @param sTable: table to be restored
   * @param tTable: table to be restored to
   * @throws IOException exception
   */
  private void restoreImages(Iterator<BackupImage> it, TableName sTable, TableName tTable)
      throws IOException {
    // First image MUST be image of a FULL backup
    BackupImage image = it.next();

    String rootDir = image.getRootDir();
    String backupId = image.getBackupId();
    Path backupRoot = new Path(rootDir);
    
    // We need hFS only for full restore (see the code)
    RestoreServerUtil restoreTool = new RestoreServerUtil(conf, backupRoot, backupId);
    BackupManifest manifest = HBackupFileSystem.getManifest(sTable, conf, backupRoot, backupId);

    Path tableBackupPath = HBackupFileSystem.getTableBackupPath(sTable, backupRoot, backupId);

    boolean converted = false;

    if (manifest.getType() == BackupType.FULL || converted) {
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from "
          + (converted ? "converted" : "full") + " backup image " + tableBackupPath.toString());
      restoreTool.fullRestoreTable(tableBackupPath, sTable, tTable, converted);
      
    } else { // incremental Backup
      throw new IOException("Unexpected backup type " + image.getType());
    }

    // The rest ones are incremental
    if (it.hasNext()) {
      List<String> logDirList = new ArrayList<String>();
      while (it.hasNext()) {
        BackupImage im = it.next();
        String logBackupDir = HBackupFileSystem.getLogBackupDir(im.getRootDir(), im.getBackupId());
        logDirList.add(logBackupDir);
      }
      String logDirs = StringUtils.join(logDirList, ",");
      LOG.info("Restoring '" + sTable + "' to '" + tTable
          + "' from log dirs: " + logDirs);
      String[] sarr = new String[logDirList.size()];
      logDirList.toArray(sarr);
      Path[] paths = org.apache.hadoop.util.StringUtils.stringToPath(sarr);
      restoreTool.incrementalRestoreTable(paths, new TableName[] { sTable },
        new TableName[] { tTable });
    }
    LOG.info(sTable + " has been successfully restored to " + tTable);
  }

  /**
   * Restore operation handle each backupImage
   * @param image: backupImage
   * @param sTable: table to be restored
   * @param tTable: table to be restored to
   * @throws IOException exception
   */
  private  void restoreImage(BackupImage image, TableName sTable, TableName tTable)
      throws IOException {
    String rootDir = image.getRootDir();
    String backupId = image.getBackupId();

    Path rootPath = new Path(rootDir);
    RestoreServerUtil restoreTool = new RestoreServerUtil(conf, rootPath, backupId);
    BackupManifest manifest = HBackupFileSystem.getManifest(sTable, conf, rootPath, backupId);

    Path tableBackupPath = HBackupFileSystem.getTableBackupPath(sTable, rootPath,  backupId);

    boolean converted = false;

    if (manifest.getType() == BackupType.FULL || converted) {
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from "
          + (converted ? "converted" : "full") + " backup image " + tableBackupPath.toString());
      restoreTool.fullRestoreTable(tableBackupPath, sTable, tTable, converted);
    } else { // incremental Backup
      String logBackupDir =
          HBackupFileSystem.getLogBackupDir(image.getRootDir(), image.getBackupId());
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from incremental backup image "
          + logBackupDir);
      restoreTool.incrementalRestoreTable(new Path[]{new Path(logBackupDir)},
          new TableName[] { sTable }, new TableName[] { tTable });
    }

    LOG.info(sTable + " has been successfully restored to " + tTable);
  }

  /**
   * Restore operation. Stage 2: resolved Backup Image dependency
   * @param backupManifestMap : tableName,  Manifest
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @param autoRestore : yes, restore all the backup images on the dependency list
   * @throws IOException exception
   */
  private void restoreStage(
    HashMap<TableName, BackupManifest> backupManifestMap, TableName[] sTableArray,
    TableName[] tTableArray, boolean autoRestore) throws IOException {
    TreeSet<BackupImage> restoreImageSet = new TreeSet<BackupImage>();
    try {
      for (int i = 0; i < sTableArray.length; i++) {
        TableName table = sTableArray[i];
        BackupManifest manifest = backupManifestMap.get(table);
        if (autoRestore) {
          // Get the image list of this backup for restore in time order from old
          // to new.
          List<BackupImage> list = new ArrayList<BackupImage>();
          list.add(manifest.getBackupImage());
          List<BackupImage> depList = manifest.getDependentListByTable(table);
          list.addAll(depList);
          TreeSet<BackupImage> restoreList = new TreeSet<BackupImage>(list);
          LOG.debug("need to clear merged Image. to be implemented in future jira");
          restoreImages(restoreList.iterator(), table, tTableArray[i]);
          restoreImageSet.addAll(restoreList);
        } else {
          BackupImage image = manifest.getBackupImage();
          List<BackupImage> depList = manifest.getDependentListByTable(table);
          // The dependency list always contains self.
          if (depList != null && depList.size() > 1) {
            LOG.warn("Backup image " + image.getBackupId() + " depends on other images.\n"
                + "this operation will only restore the delta contained within backupImage "
                + image.getBackupId());
          }
          restoreImage(image, table, tTableArray[i]);
          restoreImageSet.add(image);
        }

        if (autoRestore) {
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
      }
    } catch (Exception e) {
      setFailure("Failed in restoring stage", e);
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
    if (tTableList == null || tTableList.isEmpty()) {
      tTableList = tableList;
    }
    Connection conn = env.getMasterServices().getClusterConnection();
    TableName[] tTableArray = tTableList.toArray(new TableName[tTableList.size()]);
    try (Admin admin = conn.getAdmin()) {
      switch (state) {
        case VALIDATION:

          // check the target tables
          checkTargetTables(tTableArray, isOverwrite);

          setNextState(RestoreTablesState.RESTORE_IMAGES);
          break;
        case RESTORE_IMAGES:
          TableName[] sTableArray = tableList.toArray(new TableName[tableList.size()]);
          HashMap<TableName, BackupManifest> backupManifestMap = new HashMap<>();
          // check and load backup image manifest for the tables
          Path rootPath = new Path(targetRootDir);
          HBackupFileSystem.checkImageManifestExist(backupManifestMap, sTableArray, conf, rootPath,
            backupId);
          restoreStage(backupManifestMap, sTableArray, tTableArray, autoRestore);

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
    sb.append(")");
  }

  MasterProtos.RestoreTablesRequest toRestoreTables() {
    MasterProtos.RestoreTablesRequest.Builder bldr = MasterProtos.RestoreTablesRequest.newBuilder();
    bldr.setAutoRestore(autoRestore).setOverwrite(isOverwrite).setBackupId(backupId);
    bldr.setBackupRootDir(targetRootDir);
    for (TableName table : tableList) {
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
    autoRestore = proto.getAutoRestore();
    isOverwrite = proto.getOverwrite();
    tableList = new ArrayList<>(proto.getTablesList().size());
    for (HBaseProtos.TableName table : proto.getTablesList()) {
      tableList.add(ProtobufUtil.toTableName(table));
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
