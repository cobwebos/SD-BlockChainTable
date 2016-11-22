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
import java.net.URI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.backup.impl.BackupCommands;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.util.LogUtils;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupDriver extends AbstractHBaseTool implements BackupRestoreConstants {

  private static final Log LOG = LogFactory.getLog(BackupDriver.class);
  private CommandLine cmd;

  public BackupDriver() throws IOException {
    init();
  }

  protected void init() throws IOException {
    // disable irrelevant loggers to avoid it mess up command output
    LogUtils.disableZkAndClientLoggers(LOG);
  }

  private int parseAndRun(String[] args) throws IOException {

    // Check if backup is enabled
    if (!BackupManager.isBackupEnabled(getConf())) {
      System.err.println("Backup is not enabled. To enable backup, "+
          "set " +BackupRestoreConstants.BACKUP_ENABLE_KEY+"=true and restart "+
          "the cluster");
      return -1;
    }

    String cmd = null;
    String[] remainArgs = null;
    if (args == null || args.length == 0) {
      printToolUsage();
      return -1;
    } else {
      cmd = args[0];
      remainArgs = new String[args.length - 1];
      if (args.length > 1) {
        System.arraycopy(args, 1, remainArgs, 0, args.length - 1);
      }
    }

    BackupCommand type = BackupCommand.HELP;
    if (BackupCommand.CREATE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.CREATE;
    } else if (BackupCommand.HELP.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.HELP;
    } else if (BackupCommand.DELETE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.DELETE;
    } else if (BackupCommand.DESCRIBE.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.DESCRIBE;
    } else if (BackupCommand.HISTORY.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.HISTORY;
    } else if (BackupCommand.PROGRESS.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.PROGRESS;
    } else if (BackupCommand.SET.name().equalsIgnoreCase(cmd)) {
      type = BackupCommand.SET;
    } else {
      System.out.println("Unsupported command for backup: " + cmd);
      printToolUsage();
      return -1;
    }

    // enable debug logging
    Logger backupClientLogger = Logger.getLogger("org.apache.hadoop.hbase.backup");
    if (this.cmd.hasOption(OPTION_DEBUG)) {
      backupClientLogger.setLevel(Level.DEBUG);
    } else {
      backupClientLogger.setLevel(Level.INFO);
    }

    // TODO: get rid of Command altogether?
    BackupCommands.Command command = BackupCommands.createCommand(getConf(), type, this.cmd);
    if (type == BackupCommand.CREATE && conf != null) {
      ((BackupCommands.CreateCommand) command).setConf(conf);
    }
    try {
      command.execute();
    } catch (IOException e) {
      if (e.getMessage().equals(BackupCommands.INCORRECT_USAGE)) {
        return -1;
      }
      throw e;
    }
    return 0;
  }

  @Override
  protected void addOptions() {
    // define supported options
    addOptNoArg(OPTION_DEBUG, OPTION_DEBUG_DESC);
    addOptWithArg(OPTION_TABLE, OPTION_TABLE_DESC);
    addOptWithArg(OPTION_BANDWIDTH, OPTION_BANDWIDTH_DESC);
    addOptWithArg(OPTION_WORKERS, OPTION_WORKERS_DESC);
    addOptWithArg(OPTION_RECORD_NUMBER, OPTION_RECORD_NUMBER_DESC);
    addOptWithArg(OPTION_SET, OPTION_SET_DESC);
    addOptWithArg(OPTION_PATH, OPTION_PATH_DESC);
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
    Path hbasedir = FSUtils.getRootDir(conf);
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    FSUtils.setFsDefault(conf, new Path(defaultFs));
    int ret = ToolRunner.run(conf, new BackupDriver(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws IOException {
    if (conf == null) {
      LOG.error("Tool configuration is not initialized");
      throw new NullPointerException("conf");
    }

    CommandLine cmd;
    try {
      // parse the command line arguments
      cmd = parseArgs(args);
      cmdLineArgs = args;
    } catch (Exception e) {
      System.out.println("Error when parsing command-line arguments: " + e.getMessage());
      printToolUsage();
      return EXIT_FAILURE;
    }

    if (!sanityCheckOptions(cmd)) {
      printToolUsage();
      return EXIT_FAILURE;
    }

    processOptions(cmd);

    int ret = EXIT_FAILURE;
    try {
      ret = doWork();
    } catch (Exception e) {
      LOG.error("Error running command-line tool", e);
      return EXIT_FAILURE;
    }
    return ret;
  }

  @Override
  protected boolean sanityCheckOptions(CommandLine cmd) {
    boolean success = true;
    for (String reqOpt : requiredOptions) {
      if (!cmd.hasOption(reqOpt)) {
        System.out.println("Required option -" + reqOpt + " is missing");
        success = false;
      }
    }
    return success;
  }

  protected void printToolUsage() throws IOException {
    System.out.println(BackupCommands.USAGE);
  }
}
