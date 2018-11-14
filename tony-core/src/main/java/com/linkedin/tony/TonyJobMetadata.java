/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

public class TonyJobMetadata {

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
