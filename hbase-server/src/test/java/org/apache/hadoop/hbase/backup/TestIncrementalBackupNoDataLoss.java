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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
public class TestIncrementalBackupNoDataLoss extends TestBackupBase {
  private static final Log LOG = LogFactory.getLog(TestIncrementalBackupNoDataLoss.class);

  // implement all test cases in 1 test since incremental backup/restore has dependencies
  @Test
  public void TestIncBackupRestore() throws Exception {

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    List<TableName> tables = Lists.newArrayList(table1, table2);   
    String backupIdFull = fullTableBackup(tables);
    assertTrue(checkSucceeded(backupIdFull));
    Connection conn = ConnectionFactory.createConnection(conf1);
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

    HTable t2 = (HTable) conn.getTable(table2);
    Put p2;
    for (int i = 0; i < 5; i++) {
      p2 = new Put(Bytes.toBytes("row-t2" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }

    Assert.assertThat(TEST_UTIL.countRows(t2), CoreMatchers.equalTo(NB_ROWS_IN_BATCH + 5));
    t2.close();

    // #3 - incremental backup for table1

    tables = Lists.newArrayList(table1);
    String backupIdInc1 = incrementalTableBackup(tables);
    assertTrue(checkSucceeded(backupIdInc1));

    // #4 - incremental backup for table2

    tables = Lists.newArrayList(table2);
    String backupIdInc2 = incrementalTableBackup(tables);
    assertTrue(checkSucceeded(backupIdInc2));
    // #5 - restore incremental backup for table1
    TableName[] tablesRestoreInc1 = new TableName[] { table1 };
    TableName[] tablesMapInc1 = new TableName[] { table1_restore };

    if (TEST_UTIL.getAdmin().tableExists(table1_restore)) {
      TEST_UTIL.deleteTable(table1_restore);
    }
    if (TEST_UTIL.getAdmin().tableExists(table2_restore)) {
      TEST_UTIL.deleteTable(table2_restore);
    }

    RestoreClient client = getRestoreClient();
    client.restore(BACKUP_ROOT_DIR, backupIdInc1, false, true, tablesRestoreInc1,
      tablesMapInc1, false);

    HTable hTable = (HTable) conn.getTable(table1_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH * 2));
    hTable.close();

    // #5 - restore incremental backup for table2
    
    TableName[] tablesRestoreInc2 = new TableName[] { table2 };
    TableName[] tablesMapInc2 = new TableName[] { table2_restore };

    client = getRestoreClient();
    client.restore(BACKUP_ROOT_DIR, backupIdInc2, false, true, tablesRestoreInc2,
      tablesMapInc2, false);

    hTable = (HTable) conn.getTable(table2_restore);
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH + 5));
    hTable.close();

    conn.close();
  }

}
