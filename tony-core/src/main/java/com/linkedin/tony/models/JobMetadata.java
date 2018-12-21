/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.models;

import com.linkedin.tony.Constants;
import com.linkedin.tony.util.Utils;
import java.util.Date;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class JobMetadata {
  private String id;
  private String jobLink;
  private String configLink;
  private String rmLink;
  private long started;
  private long completed;
  private String status;
  private String user;

  public JobMetadata(String id, String jobLink, String configLink, String rmLink, long started, long completed, String status, String user) {
    this.id = id;
    this.jobLink = jobLink;
    this.configLink = configLink;
    this.rmLink = rmLink;
    this.started = started;
    this.completed = completed;
    this.status = status;
    this.user = user;
  }

  public static JobMetadata newInstance(YarnConfiguration conf, String histFileName) {
    String histFileNoExt = histFileName.substring(0, histFileName.lastIndexOf('.'));
    String[] metadata = histFileNoExt.split("-");
    String id = metadata[0];
    String jobLink = "/" + Constants.JOBS_SUFFIX + "/" + id;
    String configLink = "/" + Constants.CONFIG_SUFFIX + "/" + id;
    long started = Long.parseLong(metadata[1]);
    long completed = Long.parseLong(metadata[2]);
    String user = metadata[3];
    String status = metadata[4];
    String rmLink = Utils.buildRMUrl(conf, id);
    return new JobMetadata(id, jobLink, configLink, rmLink, started, completed, status, user);
  }

  public String getId() {
    return id;
  }

  public String getJobLink() {
    return jobLink;
  }

  public String getConfigLink() {
    return configLink;
  }

  public String getRMLink() {
    return rmLink;
  }

  public Date getStartedDate() {
    return new Date(started);
  }

  public Date getCompletedDate() {
    return new Date(completed);
  }

  public String getStatus() {
    return status;
  }

  public String getUser() {
    return user;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setUser(String user) {
    this.user = user;
  }
}
