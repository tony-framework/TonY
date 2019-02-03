/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.util.List;
import java.util.Map;

/**
 * TFConfig POJO for serialization.
 */
public class TFConfig {

  private Map<String, List<String>> clusterSpec;
  private Task task;

  public static class Task {
    private String type;
    private int index;

    // Jackson needs a default constructor
    Task() { }

    Task(String type, int index) {
      this.type = type;
      this.index = index;
    }

    // Getters required for serialization
    public String getType() {
      return this.type;
    }

    public int getIndex() {
      return this.index;
    }

    // Setters required for deserialization
    public void setType(String type) {
      this.type = type;
    }

    public void setIndex(int index) {
      this.index = index;
    }
  }

  // Jackson needs a default constructor
  TFConfig() { }

  public TFConfig(Map<String, List<String>> clusterSpec, String jobName, int taskIndex) {
    this.clusterSpec = clusterSpec;
    this.task = new Task(jobName, taskIndex);
  }

  // Getters required for serialization
  public Map<String, List<String>> getCluster() {
    return this.clusterSpec;
  }

  public Task getTask() {
    return this.task;
  }

  // Setters required for deserialization
  public void setCluster(Map<String, List<String>> clusterSpec) {
    this.clusterSpec = clusterSpec;
  }

  public void setTask(Task task) {
    this.task = task;
  }
}
