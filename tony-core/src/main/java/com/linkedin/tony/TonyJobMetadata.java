/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.util.Utils;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;


public class TonyJobMetadata {
  private static final Log LOG = LogFactory.getLog(TonyJobMetadata.class);
  private String id;
  private String url;
  private long started;
  private long completed;
  private String status;
  private String user;

  public TonyJobMetadata(String id, String url, long started, long completed, String status, String user) {
    this.id = id;
    this.url = url;
    this.started = started;
    this.completed = completed;
    this.status = status;
    this.user = user;
  }

  public static TonyJobMetadata newInstance(Configuration yarnConf, String appId, long started, long completed,
      boolean status) {
    String jobStatus = status ? "SUCCEEDED" : "FAILED";
    String user = null;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.error("Failed reading from disk. Set user to null", e);
    }
    return new TonyJobMetadata(appId, Utils.buildRMUrl(yarnConf, appId), started, completed, jobStatus, user);
  }

  public String getId() {
    return id;
  }

  public long getStarted() {
    return started;
  }

  public long getCompleted() {
    return completed;
  }

  public String getUser() {
    return user;
  }

  public String getStatus() {
    return status;
  }
}
