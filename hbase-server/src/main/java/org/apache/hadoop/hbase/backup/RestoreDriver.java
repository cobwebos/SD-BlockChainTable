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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupUtil;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.LogUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RestoreDriver extends AbstractHBaseTool {

  private static final Log LOG = LogFactory.getLog(BackupDriver.class);
  private Options opt;
  private CommandLine cmd;

  private static final String OPTION_OVERWRITE = "overwrite";
  private static final String OPTION_CHECK = "check";
  private static final String OPTION_AUTOMATIC = "automatic";

  private static final String USAGE =
      "Usage: hbase restore <backup_root_path> <backup_id> <tables> [tableMapping] \n"
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
          + "only\n" + "                   this backup image is restored\n";

  protected void init() throws IOException {
    // define supported options
    opt = new Options();
    opt.addOption(OPTION_OVERWRITE, false,
        "Overwrite the data if any of the restore target tables exists");
    opt.addOption(OPTION_CHECK, false, "Check restore sequence and dependencies");
    opt.addOption(OPTION_AUTOMATIC, false, "Restore all dependencies");
    opt.addOption("debug", false, "Enable debug logging");

    // disable irrelevant loggers to avoid it mess up command output
    LogUtils.disableUselessLoggers(LOG);
  }

  private int parseAndRun(String[] args) {
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(opt, args);
    } catch (ParseException e) {
      LOG.error("Could not parse command", e);
      return -1;
    }

    // enable debug logging
    Logger backupClientLogger = Logger.getLogger("org.apache.hadoop.hbase.backup");
    if (cmd.hasOption("debug")) {
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
    if (remainArgs.length < 3) {
      System.out.println("ERROR: missing arguments");
      System.out.println(USAGE);
      return -1;
    }

    String backupRootDir = remainArgs[0];
    String backupId = remainArgs[1];
    String tables = remainArgs[2];

    String tableMapping = (remainArgs.length > 3) ? remainArgs[3] : null;

    TableName[] sTableArray = BackupUtil.parseTableNames(tables);
    TableName[] tTableArray = BackupUtil.parseTableNames(tableMapping);

    if (sTableArray != null && tTableArray != null && (sTableArray.length != tTableArray.length)) {
      System.err.println("ERROR: table mapping mismatch: " + tables + " : " + tableMapping);
      System.out.println(USAGE);
      return -1;
    }

    try {
      RestoreClient client = BackupRestoreFactory.getRestoreClient(conf);
      client.restore(backupRootDir, backupId, check, autoRestore, sTableArray,
        tTableArray, isOverwrite);
    } catch (IOException e) {
      System.err.println("ERROR: " + e.getMessage());
      return -1;
    }
    return 0;
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
    init();
    return parseAndRun(cmd.getArgs());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new BackupDriver(), args);
    System.exit(ret);
  }
}
