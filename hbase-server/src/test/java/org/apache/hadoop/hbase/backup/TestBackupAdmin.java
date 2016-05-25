/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BackupAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupAdmin extends TestBackupBase {
  private static final Log LOG = LogFactory.getLog(TestBackupAdmin.class);
  //implement all test cases in 1 test since incremental backup/restore has dependencies
  @Test
  public void TestIncBackupRestoreWithAdminAPI() throws Exception {
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tables = Lists.newArrayList(table1, table2, table3, table4);
    HBaseAdmin admin = null;
    BackupAdmin backupAdmin = null;
    Connection conn = ConnectionFactory.createConnection(conf1);
    admin = (HBaseAdmin) conn.getAdmin();
    backupAdmin =  admin.getBackupAdmin();
    BackupRequest request = new BackupRequest();
    request.setBackupType(BackupType.FULL).setTableList(tables).setTargetRootDir(BACKUP_ROOT_DIR);
    String backupIdFull = backupAdmin.backupTables(request);

    assertTrue(checkSucceeded(backupIdFull));

    // #2 - insert some data to table
    HTable t1 = (HTable) conn.getTable(table1);
    Put p1;
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p1 = new Put(Bytes.toBytes("row-t1" + i));
      p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t1.put(p1);
    }

    Assert.assertThat(TEST_UTIL.countRows(t1), CoreMatchers.equalTo(NB_ROWS_IN_BATCH * 2));
    t1.close();

    HTable t2 =  (HTable) conn.getTable(table2);
    Put p2;
    for (int i = 0; i < 5; i++) {
      p2 = new Put(Bytes.toBytes("row-t2" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }

    Assert.assertThat(TEST_UTIL.countRows(t2), CoreMatchers.equalTo(NB_ROWS_IN_BATCH + 5));
    t2.close();

    // #3 - incremental backup for multiple tables
    tables = Lists.newArrayList(table1, table2, table3);
    request = new BackupRequest();
    request.setBackupType(BackupType.INCREMENTAL).setTableList(tables)
    .setTargetRootDir(BACKUP_ROOT_DIR);
    String backupIdIncMultiple = backupAdmin.backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple));

    // #4 - restore full backup for all tables, without overwrite
    TableName[] tablesRestoreFull =
        new TableName[] { table1, table2, table3, table4 };

    TableName[] tablesMapFull =
        new TableName[] { table1_restore, table2_restore, table3_restore, table4_restore };

    RestoreRequest restoreRequest = new RestoreRequest();
    restoreRequest.setBackupRootDir(BACKUP_ROOT_DIR).setBackupId(backupIdFull).
      setCheck(false).setAutorestore(false).setOverwrite(false).
      setFromTables(tablesRestoreFull).setToTables(tablesMapFull);
    
    backupAdmin.restore(restoreRequest);
    
    // #5.1 - check tables for full restore
    
    assertTrue(admin.tableExists(table1_restore));
    assertTrue(admin.tableExists(table2_restore));
    assertTrue(admin.tableExists(table3_restore));
    assertTrue(admin.tableExists(table4_restore));


    // #5.2 - checking row count of tables for full restore
    HTable hTable = (HTable) conn.getTable(table1_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH));
    hTable.close();

    hTable = (HTable) conn.getTable(table2_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH));
    hTable.close();

    hTable = (HTable) conn.getTable(table3_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();

    hTable = (HTable) conn.getTable(table4_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();

    // #6 - restore incremental backup for multiple tables, with overwrite
    TableName[] tablesRestoreIncMultiple =
        new TableName[] { table1, table2, table3 };
    TableName[] tablesMapIncMultiple =
        new TableName[] { table1_restore, table2_restore, table3_restore };
    
    restoreRequest = new RestoreRequest();
    restoreRequest.setBackupRootDir(BACKUP_ROOT_DIR).setBackupId(backupIdIncMultiple).
      setCheck(false).setAutorestore(false).setOverwrite(true).
      setFromTables(tablesRestoreIncMultiple).setToTables(tablesMapIncMultiple);
    
    backupAdmin.restore(restoreRequest);
    
    hTable = (HTable) conn.getTable(table1_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH * 2));
    hTable.close();

    hTable = (HTable) conn.getTable(table2_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH + 5));
    hTable.close();

    hTable = (HTable) conn.getTable(table3_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();

    // #7 - incremental backup for single, empty table

    tables = toList(table4.getNameAsString());
    request = new BackupRequest();
    request.setBackupType(BackupType.INCREMENTAL).setTableList(tables)
    .setTargetRootDir(BACKUP_ROOT_DIR);
    String backupIdIncEmpty = admin.getBackupAdmin().backupTables(request);


    // #8 - restore incremental backup for single empty table, with overwrite
    TableName[] tablesRestoreIncEmpty = new TableName[] { table4 };
    TableName[] tablesMapIncEmpty = new TableName[] { table4_restore };
    
    restoreRequest = new RestoreRequest();
    restoreRequest.setBackupRootDir(BACKUP_ROOT_DIR).setBackupId(backupIdIncEmpty).
      setCheck(false).setAutorestore(false).setOverwrite(true).
      setFromTables(tablesRestoreIncEmpty).setToTables(tablesMapIncEmpty);
    
    backupAdmin.restore(restoreRequest);   

    hTable = (HTable) conn.getTable(table4_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();
    admin.close();
    conn.close();
  }

}
