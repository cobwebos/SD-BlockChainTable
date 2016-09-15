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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.impl.BackupRestoreConstants.BackupCommand;
import org.apache.hadoop.hbase.backup.util.BackupClientUtil;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BackupAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.collect.Lists;

/**
 * General backup commands, options and usage messages
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupCommands {
  
  public final static String INCORRECT_USAGE = "Incorrect usage";

  public static final String USAGE = "Usage: hbase backup COMMAND [command-specific arguments]\n"
      + "where COMMAND is one of:\n" 
      + "  create     create a new backup image\n"
      + "  delete     delete an existing backup image\n"
      + "  describe   show the detailed information of a backup image\n"
      + "  history    show history of all successful backups\n"
      + "  progress   show the progress of the latest backup request\n"
      + "  set        backup set management\n"
      + "Run \'hbase backup COMMAND -h\' to see help message for each command\n";

  public static final String CREATE_CMD_USAGE =
      "Usage: hbase backup create <type> <BACKUP_ROOT> [tables] [-set name] "
          + "[-w workers][-b bandwith]\n" 
          + " type           \"full\" to create a full backup image\n"
          + "                \"incremental\" to create an incremental backup image\n"
          + " BACKUP_ROOT     The full root path to store the backup image,\n"
          + "                 the prefix can be hdfs, webhdfs or gpfs\n" 
          + "Options:\n"
          + " tables          If no tables (\"\") are specified, all tables are backed up.\n"
          + "                 Otherwise it is a comma separated list of tables.\n"
          + " -w              number of parallel workers (MapReduce tasks).\n" 
          + " -b              bandwith per one worker (MapReduce task) in MBs per sec\n" 
          + " -set            name of backup set to use (mutually exclusive with [tables])" ;

  public static final String PROGRESS_CMD_USAGE = "Usage: hbase backup progress <backupId>\n"
          + " backupId        backup image id\n";

  public static final String DESCRIBE_CMD_USAGE = "Usage: hbase backup decsribe <backupId>\n"
          + " backupId        backup image id\n";

  public static final String HISTORY_CMD_USAGE = 
      "Usage: hbase backup history [-path BACKUP_ROOT] [-n N] [-t table]\n"
       + " -n N            show up to N last backup sessions, default - 10\n"
       + " -path           backup root path\n"
       + " -t table        table name. If specified, only backup images which contain this table\n"
       + "                 will be listed."  ;
  

  public static final String DELETE_CMD_USAGE = "Usage: hbase backup delete <backupId>\n"
          + " backupId        backup image id\n";

  public static final String CANCEL_CMD_USAGE = "Usage: hbase backup cancel <backupId>\n"
          + " backupId        backup image id\n";

  public static final String SET_CMD_USAGE = "Usage: hbase backup set COMMAND [name] [tables]\n"
         + " name            Backup set name\n"
         + " tables          If no tables (\"\") are specified, all tables will belong to the set.\n"
         + "                 Otherwise it is a comma separated list of tables.\n"
         + "COMMAND is one of:\n" 
         + " add             add tables to a set, create a set if needed\n"
         + " remove          remove tables from a set\n"
         + " list            list all backup sets in the system\n"
         + " describe        describe set\n"
         + " delete          delete backup set\n";

  public static abstract class Command extends Configured {
    CommandLine cmdline;
    
    Command(Configuration conf) {
      super(conf);
    }
    
    public void execute() throws IOException
    {
      if (cmdline.hasOption("h") || cmdline.hasOption("help")) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }
    
    protected abstract void printUsage();
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

  static int numOfArgs(String[] args) {
    if (args == null) return 0;
    return args.length;
  }

  public static class CreateCommand extends Command {

    CreateCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }
    
    @Override
    public void execute() throws IOException {
      super.execute();
      if (cmdline == null || cmdline.getArgs() == null) {
        System.err.println("ERROR: missing arguments");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String[] args = cmdline.getArgs();
      if (args.length < 3 || args.length > 4) {
        System.err.println("ERROR: wrong number of arguments: "+ args.length);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      if (!BackupType.FULL.toString().equalsIgnoreCase(args[1])
          && !BackupType.INCREMENTAL.toString().equalsIgnoreCase(args[1])) {
        System.err.println("ERROR: invalid backup type: "+ args[1]);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
            
      String tables = null;
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();

      // Check backup set
      if (cmdline.hasOption("set")) {
        String setName = cmdline.getOptionValue("set");
        tables = getTablesForSet(setName, conf);

        if (tables == null) {
          System.err.println("ERROR: Backup set '" + setName+ "' is either empty or does not exist");
          printUsage();
          throw new IOException(INCORRECT_USAGE);
        }
      } else {
        tables = (args.length == 4) ? args[3] : null;
      }
      int bandwidth = cmdline.hasOption('b') ? Integer.parseInt(cmdline.getOptionValue('b')) : -1;
      int workers = cmdline.hasOption('w') ? Integer.parseInt(cmdline.getOptionValue('w')) : -1;

      try (Connection conn = ConnectionFactory.createConnection(getConf());
          Admin admin = conn.getAdmin();
          BackupAdmin backupAdmin = admin.getBackupAdmin();) {
        BackupRequest request = new BackupRequest();
        request.setBackupType(BackupType.valueOf(args[1].toUpperCase()))
        .setTableList(tables != null?Lists.newArrayList(BackupClientUtil.parseTableNames(tables)): null)
        .setTargetRootDir(args[2]).setWorkers(workers).setBandwidth(bandwidth);
        String backupId = backupAdmin.backupTables(request);
        System.out.println("Backup session "+ backupId+" finished. Status: SUCCESS");
      } catch (IOException e) {
        System.err.println("Backup session finished. Status: FAILURE");
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

    @Override
    protected void printUsage() {
      System.err.println(CREATE_CMD_USAGE);      
    }
  }

  private static class HelpCommand extends Command {

    HelpCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();
      if (cmdline == null) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      String[] args = cmdline.getArgs();
      if (args == null || args.length == 0) {
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }

      if (args.length != 2) {
        System.err.println("Only supports help message of a single command type");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
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
        printUsage();
      }
    }

    @Override
    protected void printUsage() {
      System.err.println(USAGE);      
    }
  }

  private static class DescribeCommand extends Command {

    DescribeCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();
      if (cmdline == null || cmdline.getArgs() == null) {
        System.err.println("ERROR: missing arguments");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String[] args = cmdline.getArgs();
      if (args.length != 2) {
        System.err.println("ERROR: wrong number of arguments");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
            
      String backupId = args[1];
      Configuration conf = getConf() != null ? getConf() : HBaseConfiguration.create();
      try (final Connection conn = ConnectionFactory.createConnection(conf);
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();) {
        BackupInfo info = admin.getBackupInfo(backupId);
        System.out.println(info.getShortDescription());
      }
    }

    @Override
    protected void printUsage() {
      System.err.println(DESCRIBE_CMD_USAGE);      
    }
  }

  private static class ProgressCommand extends Command {

    ProgressCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();
      
      if (cmdline == null || cmdline.getArgs() == null ||
          cmdline.getArgs().length != 2) {
        System.out.println("No backup id was specified, "
            + "will retrieve the most recent (ongoing) sessions");
      }
      String[] args = cmdline.getArgs();
      if (args.length > 2) {
        System.err.println("ERROR: wrong number of arguments: " + args.length);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      
      
      String backupId = args == null ? null : args[1];
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
        int progress = admin.getProgress(backupId);
        if(progress < 0){
          System.out.println("No info was found for backup id: "+backupId);
        } else{
          System.out.println(backupId+" progress=" + progress+"%");
        }
      } 
    }

    @Override
    protected void printUsage() {
      System.err.println(PROGRESS_CMD_USAGE);      
    }
  }

  private static class DeleteCommand extends Command {
    
    DeleteCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();
      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length < 2) {
        System.err.println("No backup id(s) was specified");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
            
      String[] args = cmdline.getArgs();

      String[] backupIds = new String[args.length - 1];
      System.arraycopy(args, 1, backupIds, 0, backupIds.length);
      Configuration conf = getConf() != null ? getConf() : HBaseConfiguration.create();
      try (final Connection conn = ConnectionFactory.createConnection(conf);
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();) {
        int deleted = admin.deleteBackups(args);
        System.out.println("Deleted " + deleted + " backups. Total requested: " + args.length);
      }

    }

    @Override
    protected void printUsage() {
      System.err.println(DELETE_CMD_USAGE);      
    }
  }

// TODO Cancel command  
  
  private static class CancelCommand extends Command {

    CancelCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();
      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length < 2) {
        System.out.println("No backup id(s) was specified, will use the most recent one");
      }
      String[] args = cmdline.getArgs();
      String backupId = args == null || args.length == 0 ? null : args[1];
      Configuration conf = getConf() != null ? getConf() : HBaseConfiguration.create();
      try (final Connection conn = ConnectionFactory.createConnection(conf);
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();) {
        // TODO cancel backup
      }
    }

    @Override
    protected void printUsage() {
    }
  }

  private static class HistoryCommand extends Command {
    
    private final static int DEFAULT_HISTORY_LENGTH = 10;
    
    HistoryCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {

      super.execute();
      
      int n = parseHistoryLength();
      TableName tableName = getTableName();
      Path backupRootPath = getBackupRootPath();
      List<BackupInfo> history = null;
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      if(backupRootPath == null) {
        // Load from hbase:backup
        try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
          history = admin.getHistory(n, tableName);
        } 
      } else {
        // load from backup FS
        history = BackupClientUtil.getHistory(conf, n, tableName, backupRootPath);        
      }      
      for(BackupInfo info: history){
        System.out.println(info.getShortDescription());
      }      
    }
    
    private Path getBackupRootPath() throws IOException {
      String value = null;
      try{
        value = cmdline.getOptionValue("path");
        if (value == null) return null;
        return new Path(value);
      } catch (IllegalArgumentException e) {
        System.err.println("ERROR: Illegal argument for backup root path: "+ value);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    private TableName getTableName() throws IOException {
      String value = cmdline.getOptionValue("t"); 
      if (value == null) return null;
      try{
        return TableName.valueOf(value);
      } catch (IllegalArgumentException e){
        System.err.println("Illegal argument for table name: "+ value);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    private int parseHistoryLength() throws IOException {
      String value = cmdline.getOptionValue("n");
      try{
        if (value == null) return DEFAULT_HISTORY_LENGTH;
        return Integer.parseInt(value);
      } catch(NumberFormatException e) {
        System.err.println("Illegal argument for history length: "+ value);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    @Override
    protected void printUsage() {
      System.err.println(HISTORY_CMD_USAGE);      
    }
  }

  private static class BackupSetCommand extends Command {
    private final static String SET_ADD_CMD = "add";
    private final static String SET_REMOVE_CMD = "remove";
    private final static String SET_DELETE_CMD = "delete";
    private final static String SET_DESCRIBE_CMD = "describe";
    private final static String SET_LIST_CMD = "list";

    BackupSetCommand(Configuration conf, CommandLine cmdline) {
      super(conf);
      this.cmdline = cmdline;
    }

    @Override
    public void execute() throws IOException {
      super.execute();      
      // Command-line must have at least one element
      if (cmdline == null || cmdline.getArgs() == null || cmdline.getArgs().length < 2) {
        System.err.println("ERROR: Command line format");
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
            
      String[] args = cmdline.getArgs();
      String cmdStr = args[1];
      BackupCommand cmd = getCommand(cmdStr);

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
    }

    private void processSetList(String[] args) throws IOException {
      // List all backup set names
      // does not expect any args
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
        List<BackupSet> list = admin.listBackupSets();
        for(BackupSet bs: list){
          System.out.println(bs);
        }
      }
    }

    private void processSetDescribe(String[] args) throws IOException {
      if (args == null || args.length != 3) {
        System.err.println("ERROR: Wrong number of args for 'set describe' command: "
            + numOfArgs(args));
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String setName = args[2];
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
        BackupSet set = admin.getBackupSet(setName);
        if(set == null) {
          System.out.println("Set '"+setName+"' does not exist.");
        } else{
          System.out.println(set);
        }
      }
    }

    private void processSetDelete(String[] args) throws IOException {
      if (args == null || args.length != 3) {
        System.err.println("ERROR: Wrong number of args for 'set delete' command: "
            + numOfArgs(args));
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String setName = args[2];
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
        boolean result = admin.deleteBackupSet(setName);
        if(result){
          System.out.println("Delete set "+setName+" OK.");
        } else{
          System.out.println("Set "+setName+" does not exist");
        }
      }
    }

    private void processSetRemove(String[] args) throws IOException {
      if (args == null || args.length != 4) {
        System.err.println("ERROR: Wrong number of args for 'set remove' command: "
            + numOfArgs(args));
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      
      String setName = args[2];
      String[] tables = args[3].split(",");
      Configuration conf = getConf() != null? getConf(): HBaseConfiguration.create();
      try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
        admin.removeFromBackupSet(setName, tables);
      }
    }

    private void processSetAdd(String[] args) throws IOException {
      if (args == null || args.length != 4) {
        System.err.println("ERROR: Wrong number of args for 'set add' command: "
            + numOfArgs(args));
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
      String setName = args[2];
      String[] tables = args[3].split(",");
      TableName[] tableNames = new TableName[tables.length];
      for(int i=0; i < tables.length; i++){
        tableNames[i] = TableName.valueOf(tables[i]);
      }
      Configuration conf = getConf() != null? getConf():HBaseConfiguration.create();
      try(final Connection conn = ConnectionFactory.createConnection(conf); 
          final BackupAdmin admin = conn.getAdmin().getBackupAdmin();){
        admin.addToBackupSet(setName, tableNames);
      }
      
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
        System.err.println("ERROR: Unknown command for 'set' :" + cmdStr);
        printUsage();
        throw new IOException(INCORRECT_USAGE);
      }
    }

    @Override
    protected void printUsage() {
      System.err.println(SET_CMD_USAGE);
    }

  }  
}
