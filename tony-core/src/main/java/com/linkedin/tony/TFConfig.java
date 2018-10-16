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
  private TFTask task;

  static class TFTask {
    private String type;
    private int index;

    TFTask(String type, int index) {
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
  }

  public TFConfig(Map<String, List<String>> clusterSpec, String jobName, int taskIndex) {
    this.clusterSpec = clusterSpec;
    this.task = new TFTask(jobName, taskIndex);
  }

  // Getters required for serialization
  public Map<String, List<String>> getCluster() {
    return this.clusterSpec;
  }

  public TFTask getTask() {
    return this.task;
  }
}
