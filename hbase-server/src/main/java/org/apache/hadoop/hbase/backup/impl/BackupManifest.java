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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.protobuf.InvalidProtocolBufferException;


/**
 * Backup manifest Contains all the meta data of a backup image. The manifest info will be bundled
 * as manifest file together with data. So that each backup image will contain all the info needed
 * for restore.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BackupManifest {

  private static final Log LOG = LogFactory.getLog(BackupManifest.class);

  // manifest file name
  public static final String MANIFEST_FILE_NAME = ".backup.manifest";

  // manifest file version, current is 1.0
  public static final String MANIFEST_VERSION = "1.0";

  // backup image, the dependency graph is made up by series of backup images

  public static class BackupImage implements Comparable<BackupImage> {

    private String backupId;
    private BackupType type;
    private String rootDir;
    private List<TableName> tableList;
    private long startTs;
    private long completeTs;
    private ArrayList<BackupImage> ancestors;

    public BackupImage() {
      super();
    }

    public BackupImage(String backupId, BackupType type, String rootDir,
        List<TableName> tableList, long startTs, long completeTs) {
      this.backupId = backupId;
      this.type = type;
      this.rootDir = rootDir;
      this.tableList = tableList;
      this.startTs = startTs;
      this.completeTs = completeTs;
    }

    static BackupImage fromProto(BackupProtos.BackupImage im) {
      String backupId = im.getBackupId();
      String rootDir = im.getRootDir();
      long startTs = im.getStartTs();
      long completeTs = im.getCompleteTs();
      List<HBaseProtos.TableName> tableListList = im.getTableListList();
      List<TableName> tableList = new ArrayList<TableName>();
      for(HBaseProtos.TableName tn : tableListList) {
        tableList.add(ProtobufUtil.toTableName(tn));
      }
      BackupType type =
          im.getBackupType() == BackupProtos.BackupType.FULL ? BackupType.FULL:
            BackupType.INCREMENTAL;

      return new BackupImage(backupId, type, rootDir, tableList, startTs, completeTs);
    }

    BackupProtos.BackupImage toProto() {
      BackupProtos.BackupImage.Builder builder = BackupProtos.BackupImage.newBuilder();
      builder.setBackupId(backupId);
      builder.setCompleteTs(completeTs);
      builder.setStartTs(startTs);
      builder.setRootDir(rootDir);
      if (type == BackupType.FULL) {
        builder.setBackupType(BackupProtos.BackupType.FULL);
      } else{
        builder.setBackupType(BackupProtos.BackupType.INCREMENTAL);
      }

      for (TableName name: tableList) {
        builder.addTableList(ProtobufUtil.toProtoTableName(name));
      }

      if (ancestors != null){
        for (BackupImage im: ancestors){
          builder.addAncestors(im.toProto());
        }
      }

      return builder.build();
    }

    public String getBackupId() {
      return backupId;
    }

    public void setBackupId(String backupId) {
      this.backupId = backupId;
    }

    public BackupType getType() {
      return type;
    }

    public void setType(BackupType type) {
      this.type = type;
    }

    public String getRootDir() {
      return rootDir;
    }

    public void setRootDir(String rootDir) {
      this.rootDir = rootDir;
    }

    public List<TableName> getTableNames() {
      return tableList;
    }

    public void setTableList(List<TableName> tableList) {
      this.tableList = tableList;
    }

    public long getStartTs() {
      return startTs;
    }

    public void setStartTs(long startTs) {
      this.startTs = startTs;
    }

    public long getCompleteTs() {
      return completeTs;
    }

    public void setCompleteTs(long completeTs) {
      this.completeTs = completeTs;
    }

    public ArrayList<BackupImage> getAncestors() {
      if (this.ancestors == null) {
        this.ancestors = new ArrayList<BackupImage>();
      }
      return this.ancestors;
    }

    public void addAncestor(BackupImage backupImage) {
      this.getAncestors().add(backupImage);
    }

    public boolean hasAncestor(String token) {
      for (BackupImage image : this.getAncestors()) {
        if (image.getBackupId().equals(token)) {
          return true;
        }
      }
      return false;
    }

    public boolean hasTable(TableName table) {
      for (TableName t : tableList) {
        if (t.getNameAsString().equals(table)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int compareTo(BackupImage other) {
      String thisBackupId = this.getBackupId();
      String otherBackupId = other.getBackupId();
      Long thisTS = new Long(thisBackupId.substring(thisBackupId.lastIndexOf("_") + 1));
      Long otherTS = new Long(otherBackupId.substring(otherBackupId.lastIndexOf("_") + 1));
      return thisTS.compareTo(otherTS);
    }
  }

  // manifest version
  private String version = MANIFEST_VERSION;

  // hadoop hbase configuration
  protected Configuration config = null;

  // backup root directory
  private String rootDir = null;

  // backup image directory
  private String tableBackupDir = null;

  // backup log directory if this is an incremental backup
  private String logBackupDir = null;

  // backup token
  private String backupId;

  // backup type, full or incremental
  private BackupType type;

  // the table list for the backup
  private ArrayList<TableName> tableList;

  // actual start timestamp of the backup process
  private long startTs;

  // actual complete timestamp of the backup process
  private long completeTs;

  // total bytes for table backup image
  private long totalBytes;

  // total bytes for the backed-up logs for incremental backup
  private long logBytes;

  // the region server timestamp for tables:
  // <table, <rs, timestamp>>
  private Map<TableName, HashMap<String, Long>> incrTimeRanges;

  // dependency of this backup, including all the dependent images to do PIT recovery
  private Map<String, BackupImage> dependency;

  // the indicator of the image compaction
  private boolean isCompacted = false;
  /**
   * Construct manifest for a ongoing backup.
   * @param backupCtx The ongoing backup context
   */
  public BackupManifest(BackupContext backupCtx) {
    this.backupId = backupCtx.getBackupId();
    this.type = backupCtx.getType();
    this.rootDir = backupCtx.getTargetRootDir();
    if (this.type == BackupType.INCREMENTAL) {
      this.logBackupDir = backupCtx.getHLogTargetDir();
      this.logBytes = backupCtx.getTotalBytesCopied();
    }
    this.startTs = backupCtx.getStartTs();
    this.completeTs = backupCtx.getEndTs();
    this.loadTableList(backupCtx.getTableNames());
  }

  /**
   * Construct a table level manifest for a backup of the named table.
   * @param backupCtx The ongoing backup context
   */
  public BackupManifest(BackupContext backupCtx, TableName table) {
    this.backupId = backupCtx.getBackupId();
    this.type = backupCtx.getType();
    this.rootDir = backupCtx.getTargetRootDir();
    this.tableBackupDir = backupCtx.getBackupStatus(table).getTargetDir();
    if (this.type == BackupType.INCREMENTAL) {
      this.logBackupDir = backupCtx.getHLogTargetDir();
      this.logBytes = backupCtx.getTotalBytesCopied();
    }
    this.startTs = backupCtx.getStartTs();
    this.completeTs = backupCtx.getEndTs();
    List<TableName> tables = new ArrayList<TableName>();
    tables.add(table);
    this.loadTableList(tables);
  }

  /**
   * Construct manifest from a backup directory.
   * @param conf configuration
   * @param backupPath backup path
   * @throws BackupException exception
   */

  public BackupManifest(Configuration conf, Path backupPath) throws BackupException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading manifest from: " + backupPath.toString());
    }
    // The input backupDir may not exactly be the backup table dir.
    // It could be the backup log dir where there is also a manifest file stored.
    // This variable's purpose is to keep the correct and original location so
    // that we can store/persist it.
    this.tableBackupDir = backupPath.toString();
    this.config = conf;
    try {

      FileSystem fs = backupPath.getFileSystem(conf);
      FileStatus[] subFiles = FSUtils.listStatus(fs, backupPath);
      if (subFiles == null) {
        String errorMsg = backupPath.toString() + " does not exist";
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
      for (FileStatus subFile : subFiles) {
        if (subFile.getPath().getName().equals(MANIFEST_FILE_NAME)) {

          // load and set manifest field from file content
          FSDataInputStream in = fs.open(subFile.getPath());
          long len = subFile.getLen();
          byte[] pbBytes = new byte[(int) len];
          in.readFully(pbBytes);
          BackupProtos.BackupManifest proto = null;
          try{
            proto = parseFrom(pbBytes);
          } catch(Exception e){
            throw new BackupException(e);
          }
          this.version = proto.getVersion();
          this.backupId = proto.getBackupId();
          this.type = BackupType.valueOf(proto.getType().name());
          // Here the parameter backupDir is where the manifest file is.
          // There should always be a manifest file under:
          // backupRootDir/namespace/table/backupId/.backup.manifest
          this.rootDir = backupPath.getParent().getParent().getParent().toString();

          Path p = backupPath.getParent();
          if (p.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
            this.rootDir = p.getParent().toString();
          } else {
            this.rootDir = p.getParent().getParent().toString();
          }

          loadTableList(proto);
          this.startTs = proto.getStartTs();
          this.completeTs = proto.getCompleteTs();
          this.totalBytes = proto.getTotalBytes();
          if (this.type == BackupType.INCREMENTAL) {
            this.logBytes = proto.getLogBytes();
            //TODO: convert will be implemented by future jira
          }

          loadIncrementalTimestampMap(proto);
          loadDependency(proto);
          this.isCompacted = proto.getCompacted();
          //TODO: merge will be implemented by future jira
          LOG.debug("Loaded manifest instance from manifest file: "
              + FSUtils.getPath(subFile.getPath()));
          return;
        }
      }
      String errorMsg = "No manifest file found in: " + backupPath.toString();
      LOG.error(errorMsg);
      throw new IOException(errorMsg);

    } catch (IOException e) {
      LOG.error(e);
      throw new BackupException(e.getMessage());
    }
  }

  private void loadIncrementalTimestampMap(BackupProtos.BackupManifest proto) {
    List<BackupProtos.TableServerTimestamp> list = proto.getTstMapList();
    if(list == null || list.size() == 0) return;
    this.incrTimeRanges = new HashMap<TableName, HashMap<String, Long>>();
    for(BackupProtos.TableServerTimestamp tst: list){
      TableName tn = ProtobufUtil.toTableName(tst.getTable());
      HashMap<String, Long> map = this.incrTimeRanges.get(tn);
      if(map == null){
        map = new HashMap<String, Long>();
        this.incrTimeRanges.put(tn, map);
      }
      List<BackupProtos.ServerTimestamp> listSt = tst.getServerTimestampList();
      for(BackupProtos.ServerTimestamp stm: listSt) {
        map.put(stm.getServer(), stm.getTimestamp());
      }
    }
  }

  private void loadDependency(BackupProtos.BackupManifest proto) {
    dependency = new HashMap<String, BackupImage>();
    List<BackupProtos.BackupImage> list = proto.getDependentBackupImageList();
    for (BackupProtos.BackupImage im : list) {
      dependency.put(im.getBackupId(), BackupImage.fromProto(im));
    }
  }

  private void loadTableList(BackupProtos.BackupManifest proto) {
    this.tableList = new ArrayList<TableName>();
    List<HBaseProtos.TableName> list = proto.getTableListList();
    for (HBaseProtos.TableName name: list) {
      this.tableList.add(ProtobufUtil.toTableName(name));
    }
  }

  public BackupType getType() {
    return type;
  }

  public void setType(BackupType type) {
    this.type = type;
  }

  /**
   * Loads table list.
   * @param tableList Table list
   */
  private void loadTableList(List<TableName> tableList) {

    this.tableList = this.getTableList();
    if (this.tableList.size() > 0) {
      this.tableList.clear();
    }
    for (int i = 0; i < tableList.size(); i++) {
      this.tableList.add(tableList.get(i));
    }

    LOG.debug(tableList.size() + " tables exist in table set.");
  }

  /**
   * Get the table set of this image.
   * @return The table set list
   */
  public ArrayList<TableName> getTableList() {
    if (this.tableList == null) {
      this.tableList = new ArrayList<TableName>();
    }
    return this.tableList;
  }

  /**
   * Persist the manifest file.
   * @throws IOException IOException when storing the manifest file.
   */

  public void store(Configuration conf) throws BackupException {
    byte[] data = toByteArray();
    // write the file, overwrite if already exist
    Path manifestFilePath =
        new Path(new Path((this.tableBackupDir != null ? this.tableBackupDir : this.logBackupDir))
            ,MANIFEST_FILE_NAME);
    try {
      FSDataOutputStream out =
          manifestFilePath.getFileSystem(conf).create(manifestFilePath, true);
      out.write(data);
      out.close();
    } catch (IOException e) {
      LOG.error(e);
      throw new BackupException(e.getMessage());
    }

    LOG.debug("Manifestfilestored_to " + this.tableBackupDir != null ? this.tableBackupDir
        : this.logBackupDir + Path.SEPARATOR + MANIFEST_FILE_NAME);
  }

  /**
   * Protobuf serialization
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    BackupProtos.BackupManifest.Builder builder = BackupProtos.BackupManifest.newBuilder();
    builder.setVersion(this.version);
    builder.setBackupId(this.backupId);
    builder.setType(BackupProtos.BackupType.valueOf(this.type.name()));
    setTableList(builder);
    builder.setStartTs(this.startTs);
    builder.setCompleteTs(this.completeTs);
    builder.setTotalBytes(this.totalBytes);
    if (this.type == BackupType.INCREMENTAL) {
      builder.setLogBytes(this.logBytes);
    }
    setIncrementalTimestampMap(builder);
    setDependencyMap(builder);
    builder.setCompacted(this.isCompacted);
    return builder.build().toByteArray();
  }

  private void setIncrementalTimestampMap(BackupProtos.BackupManifest.Builder builder) {
    if (this.incrTimeRanges == null) return;
    for (Entry<TableName, HashMap<String,Long>> entry: this.incrTimeRanges.entrySet()) {
      TableName key = entry.getKey();
      HashMap<String, Long> value = entry.getValue();
      BackupProtos.TableServerTimestamp.Builder tstBuilder =
          BackupProtos.TableServerTimestamp.newBuilder();
      tstBuilder.setTable(ProtobufUtil.toProtoTableName(key));

      for (String s : value.keySet()) {
        BackupProtos.ServerTimestamp.Builder stBuilder = BackupProtos.ServerTimestamp.newBuilder();
        stBuilder.setServer(s);
        stBuilder.setTimestamp(value.get(s));
        tstBuilder.addServerTimestamp(stBuilder.build());
      }
      builder.addTstMap(tstBuilder.build());
    }
  }

  private void setDependencyMap(BackupProtos.BackupManifest.Builder builder) {
    for (BackupImage image: getDependency().values()) {
      builder.addDependentBackupImage(image.toProto());
    }
  }

  private void setTableList(BackupProtos.BackupManifest.Builder builder) {
    for(TableName name: tableList){
      builder.addTableList(ProtobufUtil.toProtoTableName(name));
    }
  }

  /**
   * Parse protobuf from byte array
   * @param pbBytes A pb serialized BackupManifest instance
   * @return An instance of  made from <code>bytes</code>
   * @throws DeserializationException
   */
  private static BackupProtos.BackupManifest parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    BackupProtos.BackupManifest proto;
    try {
      proto = BackupProtos.BackupManifest.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return proto;
  }

  /**
   * Get manifest file version
   * @return version
   */
  public String getVersion() {
    return version;
  }

  /**
   * Get this backup image.
   * @return the backup image.
   */
  public BackupImage getBackupImage() {
    return this.getDependency().get(this.backupId);
  }

  /**
   * Add dependent backup image for this backup.
   * @param image The direct dependent backup image
   */
  public void addDependentImage(BackupImage image) {
    this.getDependency().get(this.backupId).addAncestor(image);
    this.setDependencyMap(this.getDependency(), image);
  }



  /**
   * Get all dependent backup images. The image of this backup is also contained.
   * @return The dependent backup images map
   */
  public Map<String, BackupImage> getDependency() {
    if (this.dependency == null) {
      this.dependency = new HashMap<String, BackupImage>();
      LOG.debug(this.rootDir + " " + this.backupId + " " + this.type);
      this.dependency.put(this.backupId,
        new BackupImage(this.backupId, this.type, this.rootDir, tableList, this.startTs,
            this.completeTs));
    }
    return this.dependency;
  }

  /**
   * Set the incremental timestamp map directly.
   * @param incrTimestampMap timestamp map
   */
  public void setIncrTimestampMap(HashMap<TableName, HashMap<String, Long>> incrTimestampMap) {
    this.incrTimeRanges = incrTimestampMap;
  }


  public Map<TableName, HashMap<String, Long>> getIncrTimestampMap() {
    if (this.incrTimeRanges == null) {
      this.incrTimeRanges = new HashMap<TableName, HashMap<String, Long>>();
    }
    return this.incrTimeRanges;
  }


  /**
   * Get the image list of this backup for restore in time order.
   * @param reverse If true, then output in reverse order, otherwise in time order from old to new
   * @return the backup image list for restore in time order
   */
  public ArrayList<BackupImage> getRestoreDependentList(boolean reverse) {
    TreeMap<Long, BackupImage> restoreImages = new TreeMap<Long, BackupImage>();
    for (BackupImage image : this.getDependency().values()) {
      restoreImages.put(Long.valueOf(image.startTs), image);
    }
    return new ArrayList<BackupImage>(reverse ? (restoreImages.descendingMap().values())
        : (restoreImages.values()));
  }

  /**
   * Get the dependent image list for a specific table of this backup in time order from old to new
   * if want to restore to this backup image level.
   * @param table table
   * @return the backup image list for a table in time order
   */
  public ArrayList<BackupImage> getDependentListByTable(TableName table) {
    ArrayList<BackupImage> tableImageList = new ArrayList<BackupImage>();
    ArrayList<BackupImage> imageList = getRestoreDependentList(true);
    for (BackupImage image : imageList) {
      if (image.hasTable(table)) {
        tableImageList.add(image);
        if (image.getType() == BackupType.FULL) {
          break;
        }
      }
    }
    Collections.reverse(tableImageList);
    return tableImageList;
  }

  /**
   * Get the full dependent image list in the whole dependency scope for a specific table of this
   * backup in time order from old to new.
   * @param table table
   * @return the full backup image list for a table in time order in the whole scope of the
   *         dependency of this image
   */
  public ArrayList<BackupImage> getAllDependentListByTable(TableName table) {
    ArrayList<BackupImage> tableImageList = new ArrayList<BackupImage>();
    ArrayList<BackupImage> imageList = getRestoreDependentList(false);
    for (BackupImage image : imageList) {
      if (image.hasTable(table)) {
        tableImageList.add(image);
      }
    }
    return tableImageList;
  }


  /**
   * Recursively set the dependency map of the backup images.
   * @param map The dependency map
   * @param image The backup image
   */
  private void setDependencyMap(Map<String, BackupImage> map, BackupImage image) {
    if (image == null) {
      return;
    } else {
      map.put(image.getBackupId(), image);
      for (BackupImage img : image.getAncestors()) {
        setDependencyMap(map, img);
      }
    }
  }

  /**
   * Check whether backup image1 could cover backup image2 or not.
   * @param image1 backup image 1
   * @param image2 backup image 2
   * @return true if image1 can cover image2, otherwise false
   */
  public static boolean canCoverImage(BackupImage image1, BackupImage image2) {
    // image1 can cover image2 only when the following conditions are satisfied:
    // - image1 must not be an incremental image;
    // - image1 must be taken after image2 has been taken;
    // - table set of image1 must cover the table set of image2.
    if (image1.getType() == BackupType.INCREMENTAL) {
      return false;
    }
    if (image1.getStartTs() < image2.getStartTs()) {
      return false;
    }
    List<TableName> image1TableList = image1.getTableNames();
    List<TableName> image2TableList = image2.getTableNames();
    boolean found = false;
    for (int i = 0; i < image2TableList.size(); i++) {
      found = false;
      for (int j = 0; j < image1TableList.size(); j++) {
        if (image2TableList.get(i).equals(image1TableList.get(j))) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }

    LOG.debug("Backup image " + image1.getBackupId() + " can cover " + image2.getBackupId());
    return true;
  }

  /**
   * Check whether backup image set could cover a backup image or not.
   * @param fullImages The backup image set
   * @param image The target backup image
   * @return true if fullImages can cover image, otherwise false
   */
  public static boolean canCoverImage(ArrayList<BackupImage> fullImages, BackupImage image) {
    // fullImages can cover image only when the following conditions are satisfied:
    // - each image of fullImages must not be an incremental image;
    // - each image of fullImages must be taken after image has been taken;
    // - sum table set of fullImages must cover the table set of image.
    for (BackupImage image1 : fullImages) {
      if (image1.getType() == BackupType.INCREMENTAL) {
        return false;
      }
      if (image1.getStartTs() < image.getStartTs()) {
        return false;
      }
    }

    ArrayList<String> image1TableList = new ArrayList<String>();
    for (BackupImage image1 : fullImages) {
      List<TableName> tableList = image1.getTableNames();
      for (TableName table : tableList) {
        image1TableList.add(table.getNameAsString());
      }
    }
    ArrayList<String> image2TableList = new ArrayList<String>();
    List<TableName> tableList = image.getTableNames();
    for (TableName table : tableList) {
      image2TableList.add(table.getNameAsString());
    }

    for (int i = 0; i < image2TableList.size(); i++) {
      if (image1TableList.contains(image2TableList.get(i)) == false) {
        return false;
      }
    }

    LOG.debug("Full image set can cover image " + image.getBackupId());
    return true;
  }
}
