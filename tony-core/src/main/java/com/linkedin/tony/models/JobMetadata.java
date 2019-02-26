/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.models;

import com.linkedin.tony.Constants;
import com.linkedin.tony.util.Utils;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
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

  private JobMetadata(JobMetadata.Builder builder) {
    this.id = builder.id;
    this.jobLink = "/" + Constants.JOBS_SUFFIX + "/" + id;
    this.configLink = "/" + Constants.CONFIG_SUFFIX + "/" + id;
    this.rmLink = Utils.buildRMUrl(builder.conf, builder.id);
    this.started = builder.started;
    this.completed = builder.completed;
    this.status = builder.status;
    this.user = builder.user;
  }

  public static JobMetadata newInstance(YarnConfiguration conf, String histFileName) {
    String histFileNoExt = histFileName.substring(0, histFileName.indexOf('.'));
    String[] metadata = histFileNoExt.split("-");
    Builder metadataBuilder = new Builder()
        .setId(metadata[0])
        .setStarted(Long.parseLong(metadata[1]))
        .setConf(conf);
    if (histFileName.endsWith(Constants.INPROGRESS)) {
      metadataBuilder
          .setUser(metadata[2])
          .setStatus(Constants.RUNNING);
      return metadataBuilder.build();
    }
    metadataBuilder
        .setCompleted(Long.parseLong(metadata[2]))
        .setUser(metadata[3])
        .setStatus(metadata[4]);
    return metadataBuilder.build();
  }

  public static class Builder {
    private String id = "";
    private long started = -1L;
    private long completed = -1L;
    private String status = "";
    private String user = "";
    private Configuration conf = null;

    public JobMetadata build() {
      return new JobMetadata(this);
    }

    public JobMetadata.Builder setId(String id) {
      this.id = id;
      return this;
    }

    public JobMetadata.Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public JobMetadata.Builder setStarted(long startTime) {
      this.started = startTime;
      return this;
    }

    public JobMetadata.Builder setCompleted(long completed) {
      this.completed = completed;
      return this;
    }

    public JobMetadata.Builder setStatus(String status) {
      this.status = status;
      return this;
    }

    public JobMetadata.Builder setUser(String user) {
      this.user = user;
      return this;
    }
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

  public long getStarted() {
    return started;
  }

  public Date getCompletedDate() {
    return new Date(completed);
  }

  public long getCompleted() {
    return completed;
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
