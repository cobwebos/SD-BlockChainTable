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

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BackupAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.hadoop.util.ToolRunner;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * An integration test to detect regressions in HBASE-7912. Create
 * a table with many regions, load data, perform series backup/load operations,
 * then restore and verify data
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-7912">HBASE-7912</a>
 */
@Category(IntegrationTests.class)
public class IntegrationTestBackupRestore extends IntegrationTestBase {
  private static final String CLASS_NAME = IntegrationTestBackupRestore.class.getSimpleName();
  protected static final Log LOG = LogFactory.getLog(IntegrationTestBackupRestore.class);
  protected static final TableName TABLE_NAME1 = TableName.valueOf(CLASS_NAME + ".table1");
  protected static final TableName TABLE_NAME2 = TableName.valueOf(CLASS_NAME + ".table2");
  protected static final String COLUMN_NAME = "f";
  protected static final String REGION_COUNT_KEY = "regions_per_rs";
  protected static final String REGIONSERVER_COUNT_KEY = "region_servers";
  protected static final int DEFAULT_REGION_COUNT = 10;
  protected static final int DEFAULT_REGIONSERVER_COUNT = 2;
  protected static int regionsCountPerServer;
  protected static int regionServerCount;
  protected static final String NB_ROWS_IN_BATCH_KEY = "rows_in_batch";
  protected static final int DEFAULT_NB_ROWS_IN_BATCH = 20000;
  private static int rowsInBatch;
  private static String BACKUP_ROOT_DIR = "backupIT";

  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();
    regionsCountPerServer = util.getConfiguration().getInt(REGION_COUNT_KEY, DEFAULT_REGION_COUNT);
    regionServerCount =
        util.getConfiguration().getInt(REGIONSERVER_COUNT_KEY, DEFAULT_REGIONSERVER_COUNT);
    rowsInBatch = util.getConfiguration().getInt(NB_ROWS_IN_BATCH_KEY, DEFAULT_NB_ROWS_IN_BATCH);
    LOG.info(String.format("Initializing cluster with %d region servers.", regionServerCount));
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized");    
    util.deleteTableIfAny(TABLE_NAME1);
    util.deleteTableIfAny(TABLE_NAME2);
    util.waitTableAvailable(BackupSystemTable.getTableName());
    LOG.info("Cluster ready");
  }

  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    if(util.isDistributedCluster()) {
      util.deleteTableIfAny(TABLE_NAME1);
      LOG.info("Cleaning up after test. TABLE1 done");
      util.deleteTableIfAny(TABLE_NAME2);
      LOG.info("Cleaning up after test. TABLE2 done");
      cleanUpBackupDir();
    }
    LOG.info("Restoring cluster.");
    util.restoreCluster();
    LOG.info("Cluster restored.");
  }

  private void cleanUpBackupDir() throws IOException {
    FileSystem fs = FileSystem.get(util.getConfiguration());
    fs.delete(new Path(BACKUP_ROOT_DIR), true);    
  }

  private String absolutePath(String dir) {
    String defaultFs = util.getConfiguration().get("fs.defaultFS");
    return defaultFs + Path.SEPARATOR + dir;
  }
  
  @Test
  public void testBackupRestore() throws Exception {
    BACKUP_ROOT_DIR = absolutePath(util.getDataTestDirOnTestFS() + Path.SEPARATOR + BACKUP_ROOT_DIR);
    createTable(TABLE_NAME1);
    createTable(TABLE_NAME2);
    runTest();
  }

  
  private void createTable(TableName tableName) throws Exception {
    long startTime, endTime;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor[] columns = 
        new HColumnDescriptor[]{new HColumnDescriptor(COLUMN_NAME)};
    SplitAlgorithm algo = new RegionSplitter.UniformSplit();
    LOG.info(String.format("Creating table %s with %d splits.", tableName, 
      regionsCountPerServer));
    startTime = System.currentTimeMillis();
    HBaseTestingUtility.createPreSplitLoadTestTable(util.getConfiguration(), desc, columns, 
      algo, regionsCountPerServer);
    util.waitTableAvailable(tableName);
    endTime = System.currentTimeMillis();
    LOG.info(String.format("Pre-split table created successfully in %dms.", 
      (endTime - startTime)));
  }

  private void loadData(TableName table, int numRows) throws IOException {
    Connection conn = util.getConnection();
    // #0- insert some data to a table
    HTable t1 = (HTable) conn.getTable(table);
    util.loadRandomRows(t1, new byte[]{'f'}, 100, numRows);
    t1.flushCommits();
  }

  private void runTest() throws IOException {
    Connection conn = util.getConnection();    

    // #0- insert some data to table TABLE_NAME1, TABLE_NAME2
    loadData(TABLE_NAME1, rowsInBatch);
    loadData(TABLE_NAME2, rowsInBatch);
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    List<TableName> tables = Lists.newArrayList(TABLE_NAME1, TABLE_NAME2);
    HBaseAdmin admin = null;
    admin = (HBaseAdmin) conn.getAdmin();
    BackupRequest request = new BackupRequest();
    request.setBackupType(BackupType.FULL).setTableList(tables).setTargetRootDir(BACKUP_ROOT_DIR);
    String backupIdFull = admin.getBackupAdmin().backupTables(request);
    assertTrue(checkSucceeded(backupIdFull));
    // #2 - insert some data to table
    loadData(TABLE_NAME1, rowsInBatch);
    loadData(TABLE_NAME2, rowsInBatch);
    HTable t1 = (HTable) conn.getTable(TABLE_NAME1);
    Assert.assertThat(util.countRows(t1), CoreMatchers.equalTo(rowsInBatch * 2));
    t1.close();
    HTable t2 = (HTable) conn.getTable(TABLE_NAME2);
    Assert.assertThat(util.countRows(t2), CoreMatchers.equalTo(rowsInBatch * 2));
    t2.close();
    // #3 - incremental backup for tables
    tables = Lists.newArrayList(TABLE_NAME1, TABLE_NAME2);
    request = new BackupRequest();
    request.setBackupType(BackupType.INCREMENTAL).setTableList(tables)
        .setTargetRootDir(BACKUP_ROOT_DIR);
    String backupIdIncMultiple = admin.getBackupAdmin().backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple));
    // #4 - restore full backup for all tables, without overwrite
    TableName[] tablesRestoreFull = new TableName[] { TABLE_NAME1, TABLE_NAME2 };
    BackupAdmin client = util.getAdmin().getBackupAdmin();
    client.restore(createRestoreRequest(BACKUP_ROOT_DIR, backupIdFull, false, tablesRestoreFull,
      null, true));
    // #5.1 - check tables for full restore
    Admin hAdmin = util.getConnection().getAdmin();
    assertTrue(hAdmin.tableExists(TABLE_NAME1));
    assertTrue(hAdmin.tableExists(TABLE_NAME2));
    hAdmin.close();
    // #5.2 - checking row count of tables for full restore
    HTable hTable = (HTable) conn.getTable(TABLE_NAME1);
    Assert.assertThat(util.countRows(hTable), CoreMatchers.equalTo(rowsInBatch));
    hTable.close();
    hTable = (HTable) conn.getTable(TABLE_NAME2);
    Assert.assertThat(util.countRows(hTable), CoreMatchers.equalTo(rowsInBatch));
    hTable.close();
    // #6 - restore incremental backup for multiple tables, with overwrite
    TableName[] tablesRestoreIncMultiple = new TableName[] { TABLE_NAME1, TABLE_NAME2 };
    client.restore(createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple, false,
      tablesRestoreIncMultiple, null, true));
    hTable = (HTable) conn.getTable(TABLE_NAME1);
    Assert.assertThat(util.countRows(hTable), CoreMatchers.equalTo(rowsInBatch * 2));
    hTable.close();
    hTable = (HTable) conn.getTable(TABLE_NAME2);
    Assert.assertThat(util.countRows(hTable), CoreMatchers.equalTo(rowsInBatch * 2));
    hTable.close();
    admin.close();
    conn.close();
  }

  protected boolean checkSucceeded(String backupId) throws IOException {
    BackupInfo status = getBackupContext(backupId);
    if (status == null) return false;
    return status.getState() == BackupState.COMPLETE;
  }

  private BackupInfo getBackupContext(String backupId) throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(util.getConnection())) {
      BackupInfo status = table.readBackupInfo(backupId);
      return status;
    }
  }

  /**
   * Get restore request.
   */
  public RestoreRequest createRestoreRequest(String backupRootDir, String backupId, boolean check,
      TableName[] fromTables, TableName[] toTables, boolean isOverwrite) {
    RestoreRequest request = new RestoreRequest();
    request.setBackupRootDir(backupRootDir).setBackupId(backupId).setCheck(check)
        .setFromTables(fromTables).setToTables(toTables).setOverwrite(isOverwrite);
    return request;
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    LOG.debug("Initializing/checking cluster has " + regionServerCount + " servers");
    util.initializeCluster(regionServerCount);
    LOG.debug("Done initializing/checking cluster");
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    testBackupRestore();
    return 0;
  }

  @Override
  public TableName getTablename() {
    return null;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return null;
  }

  @Override
  protected void addOptions() {
    addOptWithArg(REGIONSERVER_COUNT_KEY, "Total number of region servers. Default: '"
        + DEFAULT_REGIONSERVER_COUNT + "'");
    addOptWithArg(REGION_COUNT_KEY, "Total number of regions. Default: " + DEFAULT_REGION_COUNT);
    addOptWithArg(NB_ROWS_IN_BATCH_KEY, "Total number of data rows to be loaded (per table/batch."
        + " Total number of batches=2). Default: " + DEFAULT_NB_ROWS_IN_BATCH);

  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    regionsCountPerServer =
        Integer.parseInt(cmd.getOptionValue(REGION_COUNT_KEY,
          Integer.toString(DEFAULT_REGION_COUNT)));
    regionServerCount =
        Integer.parseInt(cmd.getOptionValue(REGIONSERVER_COUNT_KEY,
          Integer.toString(DEFAULT_REGIONSERVER_COUNT)));
    rowsInBatch =
        Integer.parseInt(cmd.getOptionValue(NB_ROWS_IN_BATCH_KEY,
          Integer.toString(DEFAULT_NB_ROWS_IN_BATCH)));
    LOG.info(Objects.toStringHelper("Parsed Options").add(REGION_COUNT_KEY, regionsCountPerServer)
        .add(REGIONSERVER_COUNT_KEY, regionServerCount).add(NB_ROWS_IN_BATCH_KEY, rowsInBatch)
        .toString());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestBackupRestore(), args);
    System.exit(status);
  }
}
