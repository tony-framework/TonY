/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.Constants;
import com.linkedin.tony.models.JobMetadata;


public class HistoryFileUtils {
  public static String generateFileName(JobMetadata metadata) {
    StringBuilder sb = new StringBuilder();
    sb.append(metadata.getId());
    sb.append("-");
    sb.append(metadata.getStarted());
    sb.append("-");
    if (metadata.getCompleted() != -1L) {
      sb.append(metadata.getCompleted());
      sb.append("-");
    }
    sb.append(metadata.getUser());
    if (!metadata.getStatus().isEmpty()) {
      sb.append("-");
      sb.append(metadata.getStatus());
      sb.append("." + Constants.HISTFILE_SUFFIX);
      return sb.toString();
    }
    sb.append("." + Constants.HISTFILE_SUFFIX);
    sb.append("." + Constants.INPROGRESS);
    return sb.toString();
  }

  private HistoryFileUtils() { }
}
