/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.TonyJobMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class HistoryFileUtils {
  private static final Log LOG = LogFactory.getLog(HistoryFileUtils.class);

  public static String generateFileName(TonyJobMetadata metadata) {
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
