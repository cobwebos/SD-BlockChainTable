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

package org.apache.hadoop.hbase.backup.impl;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.impl.BackupRestoreConstants.BackupCommand;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.collect.Lists;

/**
 * General backup commands, options and usage messages
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupCommands {

  private static final String USAGE = "Usage: hbase backup COMMAND\n"
      + "where COMMAND is one of:\n" + "  create     create a new backup image\n"
      + "Enter \'help COMMAND\' to see help message for each command\n";

  private static final String CREATE_CMD_USAGE =
      "Usage: hbase backup create <type> <backup_root_path> [tables] [-convert] "
          + "\n" + " type          \"full\" to create a full backup image;\n"
          + "               \"incremental\" to create an incremental backup image\n"
          + " backup_root_path   The full root path to store the backup image,\n"
          + "                    the prefix can be hdfs, webhdfs, gpfs, etc\n" + " Options:\n"
          + "   tables      If no tables (\"\") are specified, all tables are backed up. "
          + "Otherwise it is a\n" + "               comma separated list of tables.\n"
          + "   -convert    For an incremental backup, convert WAL files to HFiles\n";

  public static abstract class Command extends Configured {
    Command(Configuration conf) {
      super(conf);
    }
    public abstract void execute() throws IOException;
  }

  private BackupCommands() {
    throw new AssertionError("Instantiating utility class...");
  }

  public static Command createCommand(Configuration conf, BackupCommand type, CommandLine cmdline) {
    Command cmd = null;
    switch (type) {
      case CREATE:
        cmd = new CreateCommand(conf, cmdline);
        break;
      case HELP:
      default:
        cmd = new HelpCommand(conf, cmdline);
        break;
    }
    return cmd;
  }

  private static class CreateCommand extends Command {
    CommandLine cmdline;

    CreateCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null) {
        System.out.println("ERROR: missing arguments");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }
      String[] args = cmdline.getArgs();
      if (args.length < 2 || args.length > 3) {
        System.out.println("ERROR: wrong number of arguments");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }

      if (!BackupType.FULL.toString().equalsIgnoreCase(args[0])
          && !BackupType.INCREMENTAL.toString().equalsIgnoreCase(args[0])) {
        System.out.println("ERROR: invalid backup type");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }

      String tables = (args.length == 3) ? args[2] : null;

      try (Connection conn = ConnectionFactory.createConnection(getConf());
          Admin admin = conn.getAdmin();) {
        BackupRequest request = new BackupRequest();
        request.setBackupType(BackupType.valueOf(args[0].toUpperCase()))
        .setTableList(Lists.newArrayList(BackupUtil.parseTableNames(tables)))
        .setTargetRootDir(args[1]);
        admin.backupTables(request);
      } catch (IOException e) {
        System.err.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }
  }

  private static class HelpCommand extends Command {
    CommandLine cmdline;

    HelpCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null) {
        System.out.println(USAGE);
        System.exit(0);
      }

      String[] args = cmdline.getArgs();
      if (args == null || args.length == 0) {
        System.out.println(USAGE);
        System.exit(0);
      }

      if (args.length != 1) {
        System.out.println("Only support check help message of a single command type");
        System.out.println(USAGE);
        System.exit(0);
      }

      String type = args[0];

      if (BackupCommand.CREATE.name().equalsIgnoreCase(type)) {
        System.out.println(CREATE_CMD_USAGE);
      } // other commands will be supported in future jira
      System.exit(0);
    }
  }

}
