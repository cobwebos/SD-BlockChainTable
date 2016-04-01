/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestFullBackup extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestFullBackup.class);

  /**
   * Verify that full backup is created on a single table with data correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupSingle() throws Exception {
    LOG.info("test full backup on a single table with data");
    List<TableName> tables = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tables);
    LOG.info("backup complete for " + backupId);
  }

  /**
   * Verify that full backup is created on multiple tables correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupMultiple() throws Exception {
    LOG.info("create full backup image on multiple tables with data");
    List<TableName> tables = Lists.newArrayList(table1, table1);
    String backupId = fullTableBackup(tables);
  }

  /**
   * Verify that full backup is created on all tables correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupAll() throws Exception {
    LOG.info("create full backup image on all tables");
    String backupId = fullTableBackup(null);
  }
}