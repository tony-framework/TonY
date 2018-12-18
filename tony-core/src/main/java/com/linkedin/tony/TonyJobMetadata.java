/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.util.Utils;
import org.apache.hadoop.conf.Configuration;


public class TonyJobMetadata {
  private String id;
  private String url;
  private Long started;
  private Long completed;
  private String status;
  private String user;

  // for testing only
  TonyJobMetadata() {
    this.id = "";
    this.url = "";
    this.started = (long) 0;
    this.completed = (long) 0;
    this.status = "";
    this.user = "";
  }

  private TonyJobMetadata(String id, String url, Long started, Long completed, String status, String user) {
    this.id = id;
    this.url = url;
    this.started = started;
    this.completed = completed;
    this.status = status;
    this.user = user;
  }

  // Since newInstance could be called when the job hasn't finished,
  // `completed` and `status` could be null
  public static TonyJobMetadata newInstance(Configuration yarnConf, String appId, Long started, Long completed,
      Boolean status, String user) {
    String jobStatus = null;
    if (status != null) {
      jobStatus = status ? "SUCCEEDED" : "FAILED";
    }
    return new TonyJobMetadata(appId, Utils.buildRMUrl(yarnConf, appId), started, completed, jobStatus, user);
  }

  public String getId() {
    return id;
  }

  public Long getStarted() {
    return started;
  }

  public Long getCompleted() {
    return completed;
  }

  public String getUser() {
    return user;
  }

  public String getStatus() {
    return status;
  }
}
