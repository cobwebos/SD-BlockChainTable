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
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupClient;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.impl.BackupRestoreConstants.BackupCommand;
import org.apache.hadoop.hbase.backup.util.BackupSet;
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
      + "where COMMAND is one of:\n" 
      + "  create     create a new backup image\n"
      + "  cancel     cancel an ongoing backup\n"
      + "  delete     delete an existing backup image\n"
      + "  describe   show the detailed information of a backup image\n"
      + "  history    show history of all successful backups\n"
      + "  progress   show the progress of the latest backup request\n"
      + "  set        backup set management\n"
      + "Enter \'help COMMAND\' to see help message for each command\n";

  private static final String CREATE_CMD_USAGE =
      "Usage: hbase backup create <type> <backup_root_path> [tables] [-s name] [-convert] "
          + "[-silent] [-w workers][-b bandwith]\n" + " type          \"full\" to create a full backup image;\n"
          + "               \"incremental\" to create an incremental backup image\n"
          + "  backup_root_path   The full root path to store the backup image,\n"
          + "                    the prefix can be hdfs, webhdfs or gpfs\n" + " Options:\n"
          + "  tables      If no tables (\"\") are specified, all tables are backed up. "
          + "Otherwise it is a\n" + "               comma separated list of tables.\n"
          + " -s name     Use the specified snapshot for full backup\n"
          + " -convert    For an incremental backup, convert WAL files to HFiles\n"
          + " -w          number of parallel workers.\n" 
          + " -b          bandwith per one worker (in MB sec)" ;

  private static final String PROGRESS_CMD_USAGE = "Usage: hbase backup progress <backupId>\n"
      + " backupId      backup image id;\n";

  private static final String DESCRIBE_CMD_USAGE = "Usage: hbase backup decsribe <backupId>\n"
      + " backupId      backup image id\n";

  private static final String HISTORY_CMD_USAGE = "Usage: hbase backup history [-n N]\n"
      + " -n N     show up to N last backup sessions, default - 10;\n";

  private static final String DELETE_CMD_USAGE = "Usage: hbase backup delete <backupId>\n"
      + " backupId      backup image id;\n";

  private static final String CANCEL_CMD_USAGE = "Usage: hbase backup progress <backupId>\n"
      + " backupId      backup image id;\n";

  private static final String SET_CMD_USAGE = "Usage: hbase set COMMAND [name] [tables]\n"
      + " name       Backup set name\n"
      + " tables      If no tables (\"\") are specified, all tables will belong to the set. "
      + "Otherwise it is a\n" + "               comma separated list of tables.\n"
      + "where COMMAND is one of:\n" 
      + "  add      add tables to a set, crete set if needed\n"
      + "  remove   remove tables from set\n"
      + "  list     list all sets\n"
      + "  describe describes set\n"
      + "  delete   delete backup set\n";

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
    case DESCRIBE:
      cmd = new DescribeCommand(conf, cmdline);
      break;
    case PROGRESS:
      cmd = new ProgressCommand(conf, cmdline);
      break;
    case DELETE:
      cmd = new DeleteCommand(conf, cmdline);
      break;
    case CANCEL:
      cmd = new CancelCommand(conf, cmdline);
      break;
    case HISTORY:
      cmd = new HistoryCommand(conf, cmdline);
      break;
    case SET:
      cmd = new BackupSetCommand(conf, cmdline);
      break;
    case HELP:
    default:
      cmd = new HelpCommand(conf, cmdline);
      break;
    }
    return cmd;
  }


  public static class CreateCommand extends Command {
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
      if (args.length < 3 || args.length > 4) {
        System.out.println("ERROR: wrong number of arguments");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }

      if (!BackupType.FULL.toString().equalsIgnoreCase(args[1])
          && !BackupType.INCREMENTAL.toString().equalsIgnoreCase(args[1])) {
        System.out.println("ERROR: invalid backup type");
        System.out.println(CREATE_CMD_USAGE);
        System.exit(-1);
      }

      String tables = null;
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();

      // Check backup set
      if (cmdline.hasOption("set")) {
        String setName = cmdline.getOptionValue("set");
        tables = getTablesForSet(setName, conf);

        if (tables == null) throw new IOException("Backup set '" + setName
          + "' is either empty or does not exist");
      } else {
        tables = (args.length == 4) ? args[3] : null;
      }
      int bandwidth = cmdline.hasOption('b') ? Integer.parseInt(cmdline.getOptionValue('b')) : -1;
      int workers = cmdline.hasOption('w') ? Integer.parseInt(cmdline.getOptionValue('w')) : -1;

      try (Connection conn = ConnectionFactory.createConnection(getConf());
          Admin admin = conn.getAdmin();) {
        BackupRequest request = new BackupRequest();
        request.setBackupType(BackupType.valueOf(args[1].toUpperCase()))
        .setTableList(tables != null?Lists.newArrayList(BackupUtil.parseTableNames(tables)): null)
        .setTargetRootDir(args[2]).setWorkers(workers).setBandwidth(bandwidth);
        admin.backupTables(request);
      } catch (IOException e) {
        throw e;
      }
    }
    private String getTablesForSet(String name, Configuration conf)
        throws IOException {
      try (final Connection conn = ConnectionFactory.createConnection(conf);
          final BackupSystemTable table = new BackupSystemTable(conn)) {
        List<TableName> tables = table.describeBackupSet(name);
        if (tables == null) return null;
        return StringUtils.join(tables, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND);        
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

      if (args.length != 2) {
        System.out.println("Only support check help message of a single command type");
        System.out.println(USAGE);
        System.exit(0);
      }

      String type = args[1];

      if (BackupCommand.CREATE.name().equalsIgnoreCase(type)) {
        System.out.println(CREATE_CMD_USAGE);
      } else if (BackupCommand.DESCRIBE.name().equalsIgnoreCase(type)) {
        System.out.println(DESCRIBE_CMD_USAGE);
      } else if (BackupCommand.HISTORY.name().equalsIgnoreCase(type)) {
        System.out.println(HISTORY_CMD_USAGE);
      } else if (BackupCommand.PROGRESS.name().equalsIgnoreCase(type)) {
        System.out.println(PROGRESS_CMD_USAGE);
      } else if (BackupCommand.DELETE.name().equalsIgnoreCase(type)) {
        System.out.println(DELETE_CMD_USAGE);
      } else if (BackupCommand.CANCEL.name().equalsIgnoreCase(type)) {
        System.out.println(CANCEL_CMD_USAGE);
      } else if (BackupCommand.SET.name().equalsIgnoreCase(type)) {
        System.out.println(SET_CMD_USAGE);
      } else {
        System.out.println("Unknown command : " + type);
        System.out.println(USAGE);
      }
      System.exit(0);
    }
  }

  private static class DescribeCommand extends Command {
    CommandLine cmdline;

    DescribeCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null) {
        System.out.println("ERROR: missing arguments");
        System.out.println(DESCRIBE_CMD_USAGE);
        System.exit(-1);
      }
      String[] args = cmdline.getArgs();
      if (args.length != 2) {
        System.out.println("ERROR: wrong number of arguments");
        System.out.println(DESCRIBE_CMD_USAGE);
        System.exit(-1);
      }

      String backupId = args[1];
      try {
        Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
        BackupClient client = BackupRestoreFactory.getBackupClient(conf);
        BackupInfo info = client.getBackupInfo(backupId);
        System.out.println(info.getShortDescription());
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }
  }

  private static class ProgressCommand extends Command {
    CommandLine cmdline;

    ProgressCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null ||
          cmdline.getArgs().length != 2) {
        System.out.println("No backup id was specified, "
            + "will retrieve the most recent (ongoing) sessions");
      }
      String[] args = cmdline.getArgs();
      if (args.length > 2) {
        System.out.println("ERROR: wrong number of arguments: " + args.length);
        System.out.println(PROGRESS_CMD_USAGE);
        System.exit(-1);
      }

      String backupId = args == null ? null : args[1];
      try {
        Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
        BackupClient client = BackupRestoreFactory.getBackupClient(conf);
        int progress = client.getProgress(backupId);
        if(progress < 0){
          System.out.println("No info was found for backup id: "+backupId);
        } else{
          System.out.println(backupId+" progress=" + progress+"%");
        }
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }
  }

  private static class DeleteCommand extends Command {
    
    CommandLine cmdline;
    DeleteCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || cmdline.getArgs() == null || 
          cmdline.getArgs().length < 2) {
        System.out.println("No backup id(s) was specified");
        System.out.println(PROGRESS_CMD_USAGE);
        System.exit(-1);
      }
      String[] args = cmdline.getArgs();
      
      String[] backupIds = new String[args.length-1];
      System.arraycopy(args, 1, backupIds, 0, backupIds.length);
      try {
        Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
        BackupClient client = BackupRestoreFactory.getBackupClient(conf);
        client.deleteBackups(args);
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }
  }

// TODO Cancel command  
  
  private static class CancelCommand extends Command {
    CommandLine cmdline;

    CancelCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      if (cmdline == null || 
          cmdline.getArgs() == null || cmdline.getArgs().length < 2) {
        System.out.println("No backup id(s) was specified, will use the most recent one");
      }
      String[] args = cmdline.getArgs();
      String backupId = args == null || args.length == 0 ? null : args[1];
      try {
        Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
        BackupClient client = BackupRestoreFactory.getBackupClient(conf);
//TODO
//        client.cancelBackup(backupId);
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }
  }

  private static class HistoryCommand extends Command {
    CommandLine cmdline;
    private final static int DEFAULT_HISTORY_LENGTH = 10;
    
    HistoryCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {

      int n = parseHistoryLength();
      try {
        Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
        BackupClient client = BackupRestoreFactory.getBackupClient(conf);
        List<BackupInfo> history = client.getHistory(n);
        for(BackupInfo info: history){
          System.out.println(info.getShortDescription());
        }
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }

    private int parseHistoryLength() {
      String value = cmdline.getOptionValue("n");
      if (value == null) return DEFAULT_HISTORY_LENGTH;
      return Integer.parseInt(value);
    }
  }

  private static class BackupSetCommand extends Command {
    private final static String SET_ADD_CMD = "add";
    private final static String SET_REMOVE_CMD = "remove";
    private final static String SET_DELETE_CMD = "delete";
    private final static String SET_DESCRIBE_CMD = "describe";
    private final static String SET_LIST_CMD = "list";

    CommandLine cmdline;

    BackupSetCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {

      // Command-line must have at least one element
      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length < 2) {
        throw new IOException("command line format");
      }
      String[] args = cmdline.getArgs();
      String cmdStr = args[1];
      BackupCommand cmd = getCommand(cmdStr);

      try {

        switch (cmd) {
        case SET_ADD:
          processSetAdd(args);
          break;
        case SET_REMOVE:
          processSetRemove(args);
          break;
        case SET_DELETE:
          processSetDelete(args);
          break;
        case SET_DESCRIBE:
          processSetDescribe(args);
          break;
        case SET_LIST:
          processSetList(args);
          break;
        default:
          break;

        }
      } catch (RuntimeException e) {
        System.out.println("ERROR: " + e.getMessage());
        System.exit(-1);
      }
    }

    private void processSetList(String[] args) throws IOException {
      // List all backup set names
      // does not expect any args
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      BackupClient client = BackupRestoreFactory.getBackupClient(conf);
      client.listBackupSets();
    }

    private void processSetDescribe(String[] args) throws IOException {
      if (args == null || args.length != 3) {
        throw new RuntimeException("Wrong number of args: "+args.length);
      }
      String setName = args[2];
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      BackupClient client = BackupRestoreFactory.getBackupClient(conf);
      BackupSet set = client.getBackupSet(setName);
      System.out.println(set);
    }

    private void processSetDelete(String[] args) throws IOException {
      if (args == null || args.length != 3) {
        throw new RuntimeException("Wrong number of args");
      }
      String setName = args[2];
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      BackupClient client = BackupRestoreFactory.getBackupClient(conf);
      boolean result = client.deleteBackupSet(setName);
      if(result){
        System.out.println("Delete set "+setName+" OK.");
      } else{
        System.out.println("Set "+setName+" does not exists");
      }
    }

    private void processSetRemove(String[] args) throws IOException {
      if (args == null || args.length != 4) {
        throw new RuntimeException("Wrong args");
      }
      String setName = args[2];
      String[] tables = args[3].split(",");
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      BackupClient client = BackupRestoreFactory.getBackupClient(conf);
      client.removeFromBackupSet(setName, tables);
    }

    private void processSetAdd(String[] args) throws IOException {
      if (args == null || args.length != 4) {
        throw new RuntimeException("Wrong args");
      }
      String setName = args[2];
      String[] tables = args[3].split(",");
      Configuration conf = getConf() != null? getConf():HBaseConfiguration.create();
      BackupClient client = BackupRestoreFactory.getBackupClient(conf);
      client.addToBackupSet(setName, tables);
    }

    private BackupCommand getCommand(String cmdStr) throws IOException {
      if (cmdStr.equals(SET_ADD_CMD)) {
        return BackupCommand.SET_ADD;
      } else if (cmdStr.equals(SET_REMOVE_CMD)) {
        return BackupCommand.SET_REMOVE;
      } else if (cmdStr.equals(SET_DELETE_CMD)) {
        return BackupCommand.SET_DELETE;
      } else if (cmdStr.equals(SET_DESCRIBE_CMD)) {
        return BackupCommand.SET_DESCRIBE;
      } else if (cmdStr.equals(SET_LIST_CMD)) {
        return BackupCommand.SET_LIST;
      } else {
        throw new IOException("Unknown command for 'set' :" + cmdStr);
      }
    }

  }
  
}
