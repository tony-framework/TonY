/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;


/**
 * The class handles all HDFS file operations
 */
public class HdfsUtils {
  private static final Log LOG = LogFactory.getLog(ParserUtils.class);

  /**
   * Check to see if HDFS path exists.
   * @param fs FileSystem object.
   * @param filePath path of file to validate.
   * @return true if path is valid, false otherwise.
   */
  public static boolean pathExists(FileSystem fs, Path filePath) {
    try {
      return fs.exists(filePath);
    } catch (IOException e) {
      LOG.error("Error when reading " + filePath.toString(), e);
      return false;
    }
  }

  /**
   * Return a string which contains the content of file on HDFS.
   * @param fs FileSystem object.
   * @param filePath path of file to read from.
   * @return the content of the file, or empty string if errors occur during read.
   */
  public static String contentOfHdfsFile(FileSystem fs, Path filePath) {
    StringBuilder fileContent = new StringBuilder();
    try (FSDataInputStream inStrm = fs.open(filePath);
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(inStrm, StandardCharsets.UTF_8))) {
      String line;
      while ((line = bufReader.readLine()) != null) {
        fileContent.append(line);
      }
      return fileContent.toString();
    } catch (IOException e) {
      LOG.error("Couldn't read content of file from HDFS", e);
      return "";
    }
  }

  /**
   * Extract the last component of a file path.
   * @param path file path string.
   * @return the last component of a path
   */
  public static String getLastComponent(String path) {
    if (Strings.isNullOrEmpty(path)) {
      return "";
    }
    String[] folderLayers = path.split("/");
    return folderLayers[folderLayers.length - 1];
  }

  /**
   * Check to see if the path {@code p} is a job folder path from the given
   * folder name pattern {@code regex}
   * @param p Path object
   * @param regex regular expression string
   * @return true if path {@code p} is a job folder path, false otherwise.
   */
  @VisibleForTesting
  public static boolean isJobFolder(Path p, String regex) {
    return p != null && getLastComponent(p.toString()).matches(regex);
  }

  /**
   * Find all job folders recursively under {@code curr} that
   * matches {@code regex} pattern and return a list of
   * corresponding Path objects.
   * @param fs FileSystem object.
   * @param curr folder location Path object.
   * @param regex regular expression string.
   * @return list of job Path objects in {@code curr} folder.
   */
  public static List<Path> getJobFolders(FileSystem fs, Path curr, String regex) {
    List<Path> intermediateFolders = new ArrayList<>();
    if (curr == null) {
      return Collections.emptyList();
    }
    if (isJobFolder(curr, regex)) {
      intermediateFolders.add(curr);
      return intermediateFolders;
    }
    try {
      intermediateFolders = Arrays.stream(fs.listStatus(curr))
          .filter(FileStatus::isDirectory)
          .map(fileStatus -> getJobFolders(fs, fileStatus.getPath(), regex))
          .flatMap(List::stream)
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.error("Failed to traverse down history folder", e);
    }
    return intermediateFolders;
  }

  /**
   * Copies {@code src} to {@code dst}. If {@code src} does not have a scheme, it is assumed to be on local filesystem.
   * @param src  Source {@code Path}
   * @param dst  Destination {@code Path}
   * @param conf  HDFS configuration
   * @throws IOException
   */
  public static void copySrcToDest(Path src, Path dst, Configuration conf) throws IOException {
    FileSystem srcFs;
    if (src.toUri().getScheme() == null) {
      srcFs = FileSystem.getLocal(conf);
    } else {
      srcFs = src.getFileSystem(conf);
    }
    FileSystem dstFs = dst.getFileSystem(conf);
    FileUtil.copy(srcFs, src, dstFs, dst, false, true, conf);
  }

  private HdfsUtils() { }
}
