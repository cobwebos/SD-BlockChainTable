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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A collection of methods used by multiple classes to backup HBase tables.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BackupClientUtil {
  protected static final Log LOG = LogFactory.getLog(BackupClientUtil.class);
  public static final String LOGNAME_SEPARATOR = ".";

  private BackupClientUtil(){
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Check whether the backup path exist
   * @param backupStr backup
   * @param conf configuration
   * @return Yes if path exists
   * @throws IOException exception
   */
  public static boolean checkPathExist(String backupStr, Configuration conf)
    throws IOException {
    boolean isExist = false;
    Path backupPath = new Path(backupStr);
    FileSystem fileSys = backupPath.getFileSystem(conf);
    String targetFsScheme = fileSys.getUri().getScheme();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Schema of given url: " + backupStr + " is: " + targetFsScheme);
    }
    if (fileSys.exists(backupPath)) {
      isExist = true;
    }
    return isExist;
  }

  // check target path first, confirm it doesn't exist before backup
  public static void checkTargetDir(String backupRootPath, Configuration conf) throws IOException {
    boolean targetExists = false;
    try {
      targetExists = checkPathExist(backupRootPath, conf);
    } catch (IOException e) {
      String expMsg = e.getMessage();
      String newMsg = null;
      if (expMsg.contains("No FileSystem for scheme")) {
        newMsg =
            "Unsupported filesystem scheme found in the backup target url. Error Message: "
                + newMsg;
        LOG.error(newMsg);
        throw new IOException(newMsg);
      } else {
        throw e;
      }
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      throw e;
    }

    if (targetExists) {
      LOG.info("Using existing backup root dir: " + backupRootPath);
    } else {
      LOG.info("Backup root dir " + backupRootPath + " does not exist. Will be created.");
    }
  }

  /**
   * Get the min value for all the Values a map.
   * @param map map
   * @return the min value
   */
  public static <T> Long getMinValue(HashMap<T, Long> map) {
    Long minTimestamp = null;
    if (map != null) {
      ArrayList<Long> timestampList = new ArrayList<Long>(map.values());
      Collections.sort(timestampList);
      // The min among all the RS log timestamps will be kept in hbase:backup table.
      minTimestamp = timestampList.get(0);
    }
    return minTimestamp;
  }

  /**
   * TODO: verify the code
   * @param p path
   * @return host name
   * @throws IOException exception
   */
  public static String parseHostFromOldLog(Path p) throws IOException {
    String n = p.getName();
    int idx = n.lastIndexOf(LOGNAME_SEPARATOR);
    String s = URLDecoder.decode(n.substring(0, idx), "UTF8");
    return ServerName.parseHostname(s) + ":" + ServerName.parsePort(s);
  }

  /**
   * Given the log file, parse the timestamp from the file name. The timestamp is the last number.
   * @param p a path to the log file
   * @return the timestamp
   * @throws IOException exception
   */
  public static Long getCreationTime(Path p) throws IOException {
    int idx = p.getName().lastIndexOf(LOGNAME_SEPARATOR);
    if (idx < 0) {
      throw new IOException("Cannot parse timestamp from path " + p);
    }
    String ts = p.getName().substring(idx + 1);
    return Long.parseLong(ts);
  }

  public static List<String> getFiles(FileSystem fs, Path rootDir, List<String> files,
    PathFilter filter) throws FileNotFoundException, IOException {
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(rootDir, true);

    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (lfs.isDirectory()) {
        continue;
      }
      // apply filter
      if (filter.accept(lfs.getPath())) {
        files.add(lfs.getPath().toString());
      }
    }
    return files;
  }
}
