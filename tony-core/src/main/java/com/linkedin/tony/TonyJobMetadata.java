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
  private long started;
  private long completed;
  private String status;
  private String user;

  private TonyJobMetadata(TonyJobMetadata.Builder builder) {
    this.id = builder.id;
    this.url = Utils.buildRMUrl(builder.conf, builder.id);
    this.started = builder.started;
    this.completed = builder.completed;
    this.status = builder.status;
    this.user = builder.user;
  }

  public static class Builder {
    private String id;
    private long started = -1L;
    private long completed = -1L;
    private String status;
    private String user;
    private Configuration conf = new Configuration();

    public TonyJobMetadata build() {
      return new TonyJobMetadata(this);
    }

    public TonyJobMetadata.Builder setId(String id) {
      this.id = id;
      return this;
    }

    public TonyJobMetadata.Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public TonyJobMetadata.Builder setStartedTime(long startTime) {
      this.started = startTime;
      return this;
    }

    public TonyJobMetadata.Builder setCompleted(long completed) {
      this.completed = completed;
      return this;
    }

    public TonyJobMetadata.Builder setStatus(Boolean status) {
      String jobStatus = null;
      if (status != null) {
        jobStatus = status ? "SUCCEEDED" : "FAILED";
      }
      this.status = jobStatus;
      return this;
    }

    public TonyJobMetadata.Builder setUser(String user) {
      this.user = user;
      return this;
    }
  }

  // for testing only
  public TonyJobMetadata() {
    this.id = "";
    this.url = "";
    this.started = 0L;
    this.completed = 0L;
    this.status = "";
    this.user = "";
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
