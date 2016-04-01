/**
 *
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupContext;
import org.apache.hadoop.hbase.backup.impl.BackupContext.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTableHelper;
import org.apache.hadoop.hbase.backup.impl.BackupUtil.BackupCompleteData;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for hbase:backup API
 *
 */
@Category(MediumTests.class)
public class TestBackupSystemTable {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static Configuration conf = UTIL.getConfiguration();
  protected static MiniHBaseCluster cluster;
  protected static Connection conn;
  protected BackupSystemTable table;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = UTIL.startMiniCluster();
    conn = ConnectionFactory.createConnection(UTIL.getConfiguration());
  }

  @Before
  public void before() throws IOException {
    table = new BackupSystemTable(conn);
  }

  @After
  public void after() {
    if (table != null) {
      table.close();
    }
  }

  @Test
  public void testUpdateReadDeleteBackupStatus() throws IOException {
    BackupContext ctx = createBackupContext();
    table.updateBackupStatus(ctx);
    BackupContext readCtx = table.readBackupStatus(ctx.getBackupId());
    assertTrue(compare(ctx, readCtx));

    // try fake backup id
    readCtx = table.readBackupStatus("fake");

    assertNull(readCtx);
    // delete backup context
    table.deleteBackupStatus(ctx.getBackupId());
    readCtx = table.readBackupStatus(ctx.getBackupId());
    assertNull(readCtx);
    cleanBackupTable();
  }

  @Test
  public void testWriteReadBackupStartCode() throws IOException {
    Long code = 100L;
    table.writeBackupStartCode(code);
    String readCode = table.readBackupStartCode();
    assertEquals(code, new Long(Long.parseLong(readCode)));
    cleanBackupTable();
  }

  private void cleanBackupTable() throws IOException {
    Admin admin = UTIL.getHBaseAdmin();
    admin.disableTable(TableName.BACKUP_TABLE_NAME);
    admin.truncateTable(TableName.BACKUP_TABLE_NAME, true);
    if (admin.isTableDisabled(TableName.BACKUP_TABLE_NAME)) {
      admin.enableTable(TableName.BACKUP_TABLE_NAME);
    }
  }

  @Test
  public void testBackupHistory() throws IOException {
    int n = 10;
    List<BackupContext> list = createBackupContextList(n);

    // Load data
    for (BackupContext bc : list) {
      // Make sure we set right status
      bc.setState(BackupState.COMPLETE);
      table.updateBackupStatus(bc);
    }

    // Reverse list for comparison
    Collections.reverse(list);
    ArrayList<BackupCompleteData> history = table.getBackupHistory();
    assertTrue(history.size() == n);

    for (int i = 0; i < n; i++) {
      BackupContext ctx = list.get(i);
      BackupCompleteData data = history.get(i);
      assertTrue(compare(ctx, data));
    }

    cleanBackupTable();

  }

  @Test
  public void testRegionServerLastLogRollResults() throws IOException {
    String[] servers = new String[] { "server1", "server2", "server3" };
    Long[] timestamps = new Long[] { 100L, 102L, 107L };

    for (int i = 0; i < servers.length; i++) {
      table.writeRegionServerLastLogRollResult(servers[i], timestamps[i]);
    }

    HashMap<String, Long> result = table.readRegionServerLastLogRollResult();
    assertTrue(servers.length == result.size());
    Set<String> keys = result.keySet();
    String[] keysAsArray = new String[keys.size()];
    keys.toArray(keysAsArray);
    Arrays.sort(keysAsArray);

    for (int i = 0; i < keysAsArray.length; i++) {
      assertEquals(keysAsArray[i], servers[i]);
      Long ts1 = timestamps[i];
      Long ts2 = result.get(keysAsArray[i]);
      assertEquals(ts1, ts2);
    }

    cleanBackupTable();
  }

  @Test
  public void testIncrementalBackupTableSet() throws IOException {
    TreeSet<TableName> tables1 = new TreeSet<>();

    tables1.add(TableName.valueOf("t1"));
    tables1.add(TableName.valueOf("t2"));
    tables1.add(TableName.valueOf("t3"));

    TreeSet<TableName> tables2 = new TreeSet<>();

    tables2.add(TableName.valueOf("t3"));
    tables2.add(TableName.valueOf("t4"));
    tables2.add(TableName.valueOf("t5"));

    table.addIncrementalBackupTableSet(tables1);
    TreeSet<TableName> res1 = (TreeSet<TableName>) BackupSystemTableHelper
        .getIncrementalBackupTableSet(conn);
    assertTrue(tables1.size() == res1.size());
    Iterator<TableName> desc1 = tables1.descendingIterator();
    Iterator<TableName> desc2 = res1.descendingIterator();
    while (desc1.hasNext()) {
      assertEquals(desc1.next(), desc2.next());
    }

    table.addIncrementalBackupTableSet(tables2);
    TreeSet<TableName> res2 = (TreeSet<TableName>) BackupSystemTableHelper
        .getIncrementalBackupTableSet(conn);
    assertTrue((tables2.size() + tables1.size() - 1) == res2.size());

    tables1.addAll(tables2);

    desc1 = tables1.descendingIterator();
    desc2 = res2.descendingIterator();

    while (desc1.hasNext()) {
      assertEquals(desc1.next(), desc2.next());
    }
    cleanBackupTable();

  }

  @Test
  public void testRegionServerLogTimestampMap() throws IOException {
    TreeSet<TableName> tables = new TreeSet<>();

    tables.add(TableName.valueOf("t1"));
    tables.add(TableName.valueOf("t2"));
    tables.add(TableName.valueOf("t3"));

    HashMap<String, Long> rsTimestampMap = new HashMap<String, Long>();

    rsTimestampMap.put("rs1", 100L);
    rsTimestampMap.put("rs2", 101L);
    rsTimestampMap.put("rs3", 103L);

    table.writeRegionServerLogTimestamp(tables, rsTimestampMap);

    HashMap<TableName, HashMap<String, Long>> result = table.readLogTimestampMap();

    assertTrue(tables.size() == result.size());

    for (TableName t : tables) {
      HashMap<String, Long> rstm = result.get(t);
      assertNotNull(rstm);
      assertEquals(rstm.get("rs1"), new Long(100L));
      assertEquals(rstm.get("rs2"), new Long(101L));
      assertEquals(rstm.get("rs3"), new Long(103L));
    }

    Set<TableName> tables1 = new TreeSet<>();

    tables1.add(TableName.valueOf("t3"));
    tables1.add(TableName.valueOf("t4"));
    tables1.add(TableName.valueOf("t5"));

    HashMap<String, Long> rsTimestampMap1 = new HashMap<String, Long>();

    rsTimestampMap1.put("rs1", 200L);
    rsTimestampMap1.put("rs2", 201L);
    rsTimestampMap1.put("rs3", 203L);

    table.writeRegionServerLogTimestamp(tables1, rsTimestampMap1);

    result = table.readLogTimestampMap();

    assertTrue(5 == result.size());

    for (TableName t : tables) {
      HashMap<String, Long> rstm = result.get(t);
      assertNotNull(rstm);
      if (t.equals(TableName.valueOf("t3")) == false) {
        assertEquals(rstm.get("rs1"), new Long(100L));
        assertEquals(rstm.get("rs2"), new Long(101L));
        assertEquals(rstm.get("rs3"), new Long(103L));
      } else {
        assertEquals(rstm.get("rs1"), new Long(200L));
        assertEquals(rstm.get("rs2"), new Long(201L));
        assertEquals(rstm.get("rs3"), new Long(203L));
      }
    }

    for (TableName t : tables1) {
      HashMap<String, Long> rstm = result.get(t);
      assertNotNull(rstm);
      assertEquals(rstm.get("rs1"), new Long(200L));
      assertEquals(rstm.get("rs2"), new Long(201L));
      assertEquals(rstm.get("rs3"), new Long(203L));
    }

    cleanBackupTable();

  }

  @Test
  public void testAddWALFiles() throws IOException {
    List<String> files =
        Arrays.asList("hdfs://server/WALs/srv1,101,15555/srv1,101,15555.default.1",
          "hdfs://server/WALs/srv2,102,16666/srv2,102,16666.default.2",
            "hdfs://server/WALs/srv3,103,17777/srv3,103,17777.default.3");
    String newFile = "hdfs://server/WALs/srv1,101,15555/srv1,101,15555.default.5";

    table.addWALFiles(files, "backup");

    assertTrue(table.checkWALFile(files.get(0)));
    assertTrue(table.checkWALFile(files.get(1)));
    assertTrue(table.checkWALFile(files.get(2)));
    assertFalse(table.checkWALFile(newFile));

    cleanBackupTable();
  }

  private boolean compare(BackupContext ctx, BackupCompleteData data) {

    return ctx.getBackupId().equals(data.getBackupToken())
        && ctx.getTargetRootDir().equals(data.getBackupRootPath())
        && ctx.getType().toString().equals(data.getType())
        && ctx.getStartTs() == Long.parseLong(data.getStartTime())
        && ctx.getEndTs() == Long.parseLong(data.getEndTime());

  }

  private boolean compare(BackupContext one, BackupContext two) {
    return one.getBackupId().equals(two.getBackupId()) && one.getType().equals(two.getType())
        && one.getTargetRootDir().equals(two.getTargetRootDir())
        && one.getStartTs() == two.getStartTs() && one.getEndTs() == two.getEndTs();
  }

  private BackupContext createBackupContext() {

    BackupContext ctxt =
        new BackupContext("backup_" + System.nanoTime(), BackupType.FULL,
          new TableName[] {
              TableName.valueOf("t1"), TableName.valueOf("t2"), TableName.valueOf("t3") },
          "/hbase/backup");
    ctxt.setStartTs(System.currentTimeMillis());
    ctxt.setEndTs(System.currentTimeMillis() + 1);
    return ctxt;
  }

  private List<BackupContext> createBackupContextList(int size) {
    List<BackupContext> list = new ArrayList<BackupContext>();
    for (int i = 0; i < size; i++) {
      list.add(createBackupContext());
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return list;
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) cluster.shutdown();
  }
}
