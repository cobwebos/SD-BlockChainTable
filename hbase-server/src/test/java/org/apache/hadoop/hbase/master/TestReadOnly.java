/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;

import static org.apache.hadoop.hbase.regionserver.HRegionServer.READ_ONLY_ENABLED_KEY;

@Category(MediumTests.class)
public class TestReadOnly {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final ColumnFamilyDescriptor CFD = ColumnFamilyDescriptorBuilder.of("f1");
  private static final NamespaceDescriptor NSD = NamespaceDescriptor.DEFAULT_NAMESPACE;
  private static final NamespaceDescriptor NON_DEFAULT_NSD = NamespaceDescriptor.create("newNS").build();
  private static final HTableDescriptor NON_DEFAULT_TABLE = new HTableDescriptor(
      TableName.valueOf(NON_DEFAULT_NSD.getName(), "TestReadOnly"));

  private static HMaster master;
  private static Configuration config;
  private static TableName tableName;
  private static TableDescriptor tableDescriptor;

  @ClassRule
  public static TestName name = new TestName();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    config = TEST_UTIL.getConfiguration();
    config.setBoolean(READ_ONLY_ENABLED_KEY, true);
    TEST_UTIL.startMiniCluster();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    tableName = TableName.valueOf(name.getClass().getName());
    tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).build();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (expected = AccessDeniedException.class)
  public void testCreateTableReadOnly() throws IOException {
    master.createTable(tableDescriptor, null, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testCreateNonDefaultNSTableReadOnly() throws IOException {
    master.createTable(NON_DEFAULT_TABLE, null, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testDeleteTableReadOnly() throws IOException {
    master.deleteTable(tableName, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testTruncateTableReadOnly() throws IOException {
    master.truncateTable(tableName, false, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testAddColumnReadOnly() throws IOException {
    master.addColumn(tableName, CFD, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testModifyColumnReadOnly() throws IOException {
    master.modifyColumn(tableName, CFD, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testDeleteColumnReadOnly() throws IOException {
    master.deleteColumn(tableName, CFD.getName(), 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testEnableTableReadOnly() throws IOException {
    master.enableTable(tableName, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testDisableTableReadOnly() throws IOException {
    master.disableTable(tableName, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testModifyTableReadOnly() throws IOException {
    master.modifyTable(tableName, tableDescriptor, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testCheckTableModifiableReadOnly() throws IOException {
    master.checkTableModifiable(tableName);
  }

  @Test (expected = AccessDeniedException.class)
  public void testCreateNamespaceReadOnly() throws IOException {
    master.createNamespace(NON_DEFAULT_NSD, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testModifyNamespaceReadOnly() throws IOException {
    master.modifyNamespace(NSD, 0, 0);
  }

  @Test (expected = AccessDeniedException.class)
  public void testDeleteNamespaceReadOnly() throws IOException {
    master.deleteNamespace(NSD.getName(), 0, 0);
  }
}
