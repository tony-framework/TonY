/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.minitony.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSUtils {
  private static final Log LOG = LogFactory.getLog(HDFSUtils.class);

  /**
   * Copy files under src directory recursively to dst folder.
   * @param fs a hadoop file system reference
   * @param src the directory under which you want to copy files from (local disk)
   * @param dst the destination directory. (hdfs)
   * @throws IOException exception when copy files.
   */
  public static void copyDirectoryFilesToFolder(FileSystem fs, String src, String dst) throws IOException {
    Files.walk(Paths.get(src))
        .filter(Files::isRegularFile)
        .forEach(file -> {
          Path jar = new Path(file.toString());
          try {
            fs.copyFromLocalFile(jar, new Path(dst));
          } catch (IOException e) {
            LOG.error("Failed to copy directory from: " + src + " to: " + dst + " ", e);
          }
        });
  }

  private HDFSUtils() { }
}