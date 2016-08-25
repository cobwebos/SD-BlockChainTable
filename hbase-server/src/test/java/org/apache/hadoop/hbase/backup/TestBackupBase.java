/*
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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BackupAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;


/**
 * This class is only a base for other integration-level backup tests. Do not add tests here.
 * TestBackupSmallTests is where tests that don't require bring machines up/down should go All other
 * tests should have their own classes and extend this one
 */
public class TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestBackupBase.class);

  protected static Configuration conf1;
  protected static Configuration conf2;

  protected static HBaseTestingUtility TEST_UTIL;
  protected static HBaseTestingUtility TEST_UTIL2;
  protected static TableName table1 = TableName.valueOf("table1");
  protected static TableName table2 = TableName.valueOf("table2");
  protected static TableName table3 = TableName.valueOf("table3");
  protected static TableName table4 = TableName.valueOf("table4");

  protected static TableName table1_restore = TableName.valueOf("ns1:table1_restore");
  protected static TableName table2_restore = TableName.valueOf("ns2:table2_restore");
  protected static TableName table3_restore = TableName.valueOf("ns3:table3_restore");
  protected static TableName table4_restore = TableName.valueOf("ns4:table4_restore");

  protected static final int NB_ROWS_IN_BATCH = 99;
  protected static final byte[] qualName = Bytes.toBytes("q1");
  protected static final byte[] famName = Bytes.toBytes("f");

  protected static String BACKUP_ROOT_DIR = "/backupUT";
  protected static String BACKUP_REMOTE_ROOT_DIR = "/backupUT";
  protected static String provider = "defaultProvider";

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // Set MultiWAL (with 2 default WAL files per RS)
    conf1.set(WALFactory.WAL_PROVIDER, provider);
    TEST_UTIL.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = TEST_UTIL.getZkCluster();

    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    TEST_UTIL2 = new HBaseTestingUtility(conf2);
    TEST_UTIL2.setZkCluster(miniZK);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL2.startMiniCluster();
    conf1 = TEST_UTIL.getConfiguration();

    TEST_UTIL.startMiniMapReduceCluster();
    BACKUP_ROOT_DIR = TEST_UTIL.getConfiguration().get("fs.defaultFS") + "/backupUT";
    LOG.info("ROOTDIR " + BACKUP_ROOT_DIR);
    BACKUP_REMOTE_ROOT_DIR = TEST_UTIL2.getConfiguration().get("fs.defaultFS") + "/backupUT";
    LOG.info("REMOTE ROOTDIR " + BACKUP_REMOTE_ROOT_DIR);
    waitForSystemTable();
    createTables();
  }
  
  public static void waitForSystemTable() throws Exception
  {
    try(Admin admin = TEST_UTIL.getAdmin();) {
      while (!admin.tableExists(BackupSystemTable.getTableName()) 
          || !admin.isTableAvailable(BackupSystemTable.getTableName())) {
        Thread.sleep(1000);
      }      
    }
    LOG.debug("backup table exists and available");

  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }

  protected String backupTables(BackupType type, List<TableName> tables, String path)
      throws IOException {
    Connection conn = null;
    HBaseAdmin admin = null;
    BackupAdmin badmin = null;
    String backupId;
    try {
      conn = ConnectionFactory.createConnection(conf1);
      admin = (HBaseAdmin) conn.getAdmin();
      BackupRequest request = new BackupRequest();
      request.setBackupType(type).setTableList(tables).setTargetRootDir(path);
      badmin = admin.getBackupAdmin();
      backupId = badmin.backupTables(request);
    } finally {
      if(badmin != null){
        badmin.close();
      }
      if (admin != null) {
        admin.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
    return backupId;
  }

  protected String fullTableBackup(List<TableName> tables) throws IOException {
    return backupTables(BackupType.FULL, tables, BACKUP_ROOT_DIR);
  }

  protected String incrementalTableBackup(List<TableName> tables) throws IOException {
    return backupTables(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
  }
  
  protected static void loadTable(HTable table) throws Exception {

    Put p; // 100 + 1 row to t1_syncup
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      table.put(p);
    }
  }

  protected static void createTables() throws Exception {

    long tid = System.currentTimeMillis();
    table1 = TableName.valueOf("ns1:test-" + tid);
    HBaseAdmin ha = TEST_UTIL.getHBaseAdmin();
    
    // Create namespaces
    NamespaceDescriptor desc1 = NamespaceDescriptor.create("ns1").build();
    NamespaceDescriptor desc2 = NamespaceDescriptor.create("ns2").build();
    NamespaceDescriptor desc3 = NamespaceDescriptor.create("ns3").build();
    NamespaceDescriptor desc4 = NamespaceDescriptor.create("ns4").build();
    
    ha.createNamespace(desc1);
    ha.createNamespace(desc2);
    ha.createNamespace(desc3);
    ha.createNamespace(desc4);

    
    HTableDescriptor desc = new HTableDescriptor(table1);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    desc.addFamily(fam);
    ha.createTable(desc);
    Connection conn = ConnectionFactory.createConnection(conf1);
    HTable table = (HTable) conn.getTable(table1);
    loadTable(table);
    table.close();
    table2 = TableName.valueOf("ns2:test-" + tid + 1);
    desc = new HTableDescriptor(table2);
    desc.addFamily(fam);
    ha.createTable(desc);
    table = (HTable) conn.getTable(table2);
    loadTable(table);
    table.close();
    table3 = TableName.valueOf("ns3:test-" + tid + 2);
    table = TEST_UTIL.createTable(table3, famName);
    table.close();
    table4 = TableName.valueOf("ns4:test-" + tid + 3);
    table = TEST_UTIL.createTable(table4, famName);
    table.close();
    ha.close();
    conn.close();
  }

  protected boolean checkSucceeded(String backupId) throws IOException {
    BackupInfo status = getBackupContext(backupId);
    if (status == null) return false;
    return status.getState() == BackupState.COMPLETE;
  }

  protected boolean checkFailed(String backupId) throws IOException {
    BackupInfo status = getBackupContext(backupId);
    if (status == null) return false;
    return status.getState() == BackupState.FAILED;
  }

  private BackupInfo getBackupContext(String backupId) throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      BackupInfo status = table.readBackupInfo(backupId);
      return status;
    }
  }

  protected BackupAdmin getBackupAdmin() throws IOException {
    return TEST_UTIL.getAdmin().getBackupAdmin();
  }
  
  /**
   * Get restore request.
   *  
   */
  public  RestoreRequest createRestoreRequest(
      String backupRootDir,
      String backupId, boolean check, TableName[] fromTables,
      TableName[] toTables, boolean isOverwrite) {
    RestoreRequest request = new RestoreRequest();
    request.setBackupRootDir(backupRootDir).setBackupId(backupId).setCheck(check).
    setFromTables(fromTables).setToTables(toTables).setOverwrite(isOverwrite);
    return request;
}
  
  /**
   * Helper method
   */
  protected List<TableName> toList(String... args){
    List<TableName> ret = new ArrayList<>();
    for(int i=0; i < args.length; i++){
      ret.add(TableName.valueOf(args[i]));
    }
    return ret;
  }
    
  protected void dumpBackupDir() throws IOException
  {
    // Dump Backup Dir
    FileSystem fs = FileSystem.get(conf1);
    RemoteIterator<LocatedFileStatus> it = fs.listFiles( new Path(BACKUP_ROOT_DIR), true);
    while(it.hasNext()){
      LOG.debug("DDEBUG: "+it.next().getPath());
    }

  }
}
