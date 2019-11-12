/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import java.util.List;


public class JobContainerRequest {
  private int numInstances;
  private long memory;
  private int vCores;
  private int priority;
  private int gpu;
  private String jobName;
  private String nodeLabelsExpression;
  private List<String> dependsOn;

  public JobContainerRequest(String jobName, int numInstances, long memory, int vCores, int gpu, int priority,
      String nodeLabelsExpression, final List<String> dependsOn) {
    this.numInstances = numInstances;
    this.memory = memory;
    this.vCores = vCores;
    this.priority = priority;
    this.gpu = gpu;
    this.jobName = jobName;
    this.nodeLabelsExpression = nodeLabelsExpression;
    this.dependsOn = dependsOn;
  }

  public int getNumInstances() {
    return numInstances;
  }

  public long getMemory() {
    return memory;
  }

  public int getVCores() {
    return vCores;
  }

  public int getGPU() {
    return gpu;
  }

  public int getPriority() {
    return priority;
  }

  public String getJobName() {
    return jobName;
  }

  public String getNodeLabelsExpression() {
    return nodeLabelsExpression;
  }

  public final List<String> getDependsOn() {
    return dependsOn;
  }
}
