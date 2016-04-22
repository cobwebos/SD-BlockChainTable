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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestFullBackupSet extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestFullBackupSet.class);


  /**
   * Verify that full backup is created on a single table with data correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupSetExist() throws Exception {

    LOG.info("TFBSE test full backup, backup set exists");
    
    //Create set
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      String name = "name";
      table.addToBackupSet(name, new String[] { table1.getNameAsString() });
      List<TableName> names = table.describeBackupSet(name);

      assertNotNull(names);
      assertTrue(names.size() == 1);
      assertTrue(names.get(0).equals(table1));

      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-set", name };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      ArrayList<BackupInfo> backups = table.getBackupHistory();
      assertTrue(backups.size() == 1);
      String backupId = backups.get(0).getBackupId();
      assertTrue(checkSucceeded(backupId));
      LOG.info("TFBSE backup complete");
    }

  }

  @Test
  public void testFullBackupSetDoesNotExist() throws Exception {

    LOG.info("TFBSE test full backup, backup set does not exist");    
    String name = "name1";    
    String[] args = new String[]{"create", "full", BACKUP_ROOT_DIR, "-set", name }; 
    // Run backup
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertTrue(ret != 0);

  }

}