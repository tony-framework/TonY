/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.TonyJobMetadata;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HistoryFileUtils {
  private static final Log LOG = LogFactory.getLog(HistoryFileUtils.class);

  public static void createHistoryFile(FileSystem fs, TonyJobMetadata metadata, Path jobDir) {
    if (jobDir == null) {
      return;
    }
    Path historyFile = new Path(jobDir, generateFileName(metadata));
    try {
      fs.create(historyFile);
    } catch (IOException e) {
      LOG.error("Failed to create history file", e);
    }
  }

  private static String generateFileName(TonyJobMetadata metadata) {
    StringBuilder sb = new StringBuilder();
    sb.append(metadata.getId());
    sb.append("-");
    sb.append(metadata.getStarted());
    sb.append("-");
    sb.append(metadata.getCompleted());
    sb.append("-");
    sb.append(metadata.getUser());
    sb.append("-");
    sb.append(metadata.getStatus());
    sb.append(".jhist");
    return sb.toString();
  }

  private HistoryFileUtils() { }
}
