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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupServerUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.LogUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RestoreDriver extends AbstractHBaseTool {

  private static final Log LOG = LogFactory.getLog(RestoreDriver.class);
  private CommandLine cmd;

  private static final String OPTION_OVERWRITE = "overwrite";
  private static final String OPTION_CHECK = "check";
  private static final String OPTION_AUTOMATIC = "automatic";
  private static final String OPTION_SET = "set";
  private static final String OPTION_DEBUG = "debug";


  private static final String USAGE =
      "Usage: hbase restore [-set set_name] <backup_root_path> <backup_id> <tables> [tableMapping] \n"
          + "       [-overwrite] [-check] [-automatic]\n"
          + " backup_root_path  The parent location where the backup images are stored\n"
          + " backup_id         The id identifying the backup image\n"
          + " table(s)          Table(s) from the backup image to be restored.\n"
          + "                   Tables are separated by comma.\n"
          + " Options:\n"
          + "   tableMapping    A comma separated list of target tables.\n"
          + "                   If specified, each table in <tables> must have a mapping.\n"
          + "   -overwrite      With this option, restore overwrites to the existing table "
          + "if there's any in\n"
          + "                   restore target. The existing table must be online before restore.\n"
          + "   -check          With this option, restore sequence and dependencies are checked\n"
          + "                   and verified without executing the restore\n"
          + "   -automatic      With this option, all the dependencies are automatically restored\n"
          + "                   together with this backup image following the correct order.\n"
          + "                   The restore dependencies can be checked by using \"-check\" "
          + "option,\n"
          + "                   or using \"hbase backup describe\" command. Without this option, "
          + "only\n" + "                   this backup image is restored\n"
          + "   -set set_name   Backup set to restore, mutually exclusive with table list <tables>.";

    
  protected RestoreDriver() throws IOException
  {
    init();
  }
  
  protected void init() throws IOException {
    // define supported options
    addOptNoArg(OPTION_OVERWRITE,
        "Overwrite the data if any of the restore target tables exists");
    addOptNoArg(OPTION_CHECK, "Check restore sequence and dependencies");
    addOptNoArg(OPTION_AUTOMATIC, "Restore all dependencies");
    addOptNoArg(OPTION_DEBUG,  "Enable debug logging");
    addOptWithArg(OPTION_SET, "Backup set name");

    // disable irrelevant loggers to avoid it mess up command output
    LogUtils.disableUselessLoggers(LOG);
  }

  private int parseAndRun(String[] args) {

    // enable debug logging
    Logger backupClientLogger = Logger.getLogger("org.apache.hadoop.hbase.backup");
    if (cmd.hasOption(OPTION_DEBUG)) {
      backupClientLogger.setLevel(Level.DEBUG);
    }

    // whether to overwrite to existing table if any, false by default
    boolean isOverwrite = cmd.hasOption(OPTION_OVERWRITE);
    if (isOverwrite) {
      LOG.debug("Found -overwrite option in restore command, "
          + "will overwrite to existing table if any in the restore target");
    }

    // whether to only check the dependencies, false by default
    boolean check = cmd.hasOption(OPTION_CHECK);
    if (check) {
      LOG.debug("Found -check option in restore command, "
          + "will check and verify the dependencies");
    }

    // whether to restore all dependencies, false by default
    boolean autoRestore = cmd.hasOption(OPTION_AUTOMATIC);
    if (autoRestore) {
      LOG.debug("Found -automatic option in restore command, "
          + "will automatically retore all the dependencies");
    }

    // parse main restore command options
    String[] remainArgs = cmd.getArgs();
    if (remainArgs.length < 3 && !cmd.hasOption(OPTION_SET) ||
        (cmd.hasOption(OPTION_SET) && remainArgs.length < 2)) {
      System.out.println("ERROR: remain args length="+ remainArgs.length);
      System.out.println(USAGE);
      return -1;
    } 

    String backupRootDir = remainArgs[0];
    String backupId = remainArgs[1];
    String tables = null;
    String tableMapping = null;
    // Check backup set
    if (cmd.hasOption(OPTION_SET)) {
      String setName = cmd.getOptionValue(OPTION_SET);
      try{
        tables = getTablesForSet(setName, conf);       
      } catch(IOException e){
        System.out.println("ERROR: "+ e.getMessage()+" for setName="+setName);
        return -2;
      }
      if (tables == null) {
        System.out.println("ERROR: Backup set '" + setName
        + "' is either empty or does not exist");
        return -3;
      }
      tableMapping = (remainArgs.length > 2) ? remainArgs[2] : null;
    } else {
      tables = remainArgs[2];    
      tableMapping = (remainArgs.length > 3) ? remainArgs[3] : null;
    }    

    TableName[] sTableArray = BackupServerUtil.parseTableNames(tables);
    TableName[] tTableArray = BackupServerUtil.parseTableNames(tableMapping);

    if (sTableArray != null && tTableArray != null && (sTableArray.length != tTableArray.length)) {
      System.out.println("ERROR: table mapping mismatch: " + tables + " : " + tableMapping);
      System.out.println(USAGE);
      return -4;
    }

    
    RestoreClient client = BackupRestoreClientFactory.getRestoreClient(getConf());
    try{
      client.restore(backupRootDir, backupId, check, autoRestore, sTableArray,
        tTableArray, isOverwrite);
    } catch (Exception e){
      e.printStackTrace();
      return -5;
    }
    return 0;
  }

  private String getTablesForSet(String name, Configuration conf) throws IOException {
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> tables = table.describeBackupSet(name);
      if (tables == null) return null;
      return StringUtils.join(tables, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);
    }
  }
  
  @Override
  protected void addOptions() {
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    this.cmd = cmd;
  }

  @Override
  protected int doWork() throws Exception {
    return parseAndRun(cmd.getArgs());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new RestoreDriver(), args);
    System.exit(ret);
  }
}
