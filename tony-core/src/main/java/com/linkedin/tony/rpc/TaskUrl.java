/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import java.util.Objects;


/**
 * Contains the name, index, and URL for a task.
 */
public class TaskUrl implements Comparable<TaskUrl> {
  private final String name;   // The name (worker or ps) of the task
  private final String index;  // The index of the task
  private final String url;    // The URL where the logs for the task can be found

  public TaskUrl(String name, String index, String url) {
    this.name = name;
    this.index = index;
    this.url = url;
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

  @Override
  public int compareTo(TaskUrl other) {
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
    TaskUrl taskUrl = (TaskUrl) o;
    return Objects.equals(name, taskUrl.name) && Objects.equals(index, taskUrl.index) && Objects.equals(url,
        taskUrl.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, index, url);
  }
}
