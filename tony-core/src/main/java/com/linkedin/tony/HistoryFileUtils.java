/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class HistoryFileUtils {
  private static final Log LOG = LogFactory.getLog(HistoryFileUtils.class);

  public static TonyJobMetadata createMetadataObj(Configuration yarnConf, String appId, long started, long completed,
      boolean status) {
    String jobStatus = status ? "SUCCEEDED" : "FAILED";
    String url = "http://" + yarnConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS) + "/cluster/app/" + appId;
    String user = null;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.error("Failed reading from disk. Set user to null", e);
    }
    return new TonyJobMetadata(appId, url, started, completed, jobStatus, user);
  }

  public static void createHistoryFile(FileSystem fs, TonyJobMetadata metaObj, Path jobDir) {
    Path historyFile = new Path(jobDir, generateFileName(metaObj));
    try {
      fs.create(historyFile);
    } catch (IOException e) {
      LOG.error("Failed to create history file", e);
    }
  }

  private static String generateFileName(TonyJobMetadata metadataObj) {
    StringBuilder sb = new StringBuilder();
    sb.append(metadataObj.getId());
    sb.append("-");
    sb.append(metadataObj.getStarted());
    sb.append("-");
    sb.append(metadataObj.getCompleted());
    sb.append("-");
    sb.append(metadataObj.getUser());
    sb.append("-");
    sb.append(metadataObj.getStatus());
    sb.append(".jhist");
    return sb.toString();
  }

  private HistoryFileUtils() { }
}
