/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import com.linkedin.tony.rpc.impl.TaskStatus;

import java.util.Objects;


/**
 * Contains the name, index, and URL for a task.
 */
public class TaskInfo implements Comparable<TaskInfo> {
  private final String name;   // The name (worker or ps) of the task
  private final String index;  // The index of the task
  private final String url;    // The URL where the logs for the task can be found
  private TaskStatus status = TaskStatus.NEW;

  public TaskInfo(String name, String index, String url) {
    this.name = name;
    this.index = index;
    this.url = url;
  }

  public void setState(TaskStatus status) {
    this.status = status;
  }

  public String getName() {
    return name;
  }

  public String getIndex() {
    return index;
  }

  public String getUrl() {
    return url;
  }

  public TaskStatus getStatus() {
    return status;
  }

  @Override
  public int compareTo(TaskInfo other) {
    if (!this.name.equals(other.name)) {
      return this.name.compareTo(other.name);
    }
    return Integer.valueOf(this.index).compareTo(Integer.valueOf(other.index));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskInfo taskInfo = (TaskInfo) o;
    return Objects.equals(name, taskInfo.name)
            && Objects.equals(index, taskInfo.index)
            && Objects.equals(url, taskInfo.url)
            && Objects.equals(status, taskInfo.getStatus());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, index, url, status);
  }

  @Override
  public String toString() {
    return String.format(
        "[TaskInfo] name: %s index: %s url: %s status: %s",
        this.name, this.index, this.url, this.status.toString());
  }
}
