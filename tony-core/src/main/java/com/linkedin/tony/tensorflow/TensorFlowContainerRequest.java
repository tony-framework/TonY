/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;


public class TensorFlowContainerRequest {
  private int virtualCores;
  private long memory;
  private int priority;
  private int gpu;
  private String jobName;

  public TensorFlowContainerRequest(String jobName, int virtualCores, long memory, int gpu, int priority) {
    this.virtualCores = virtualCores;
    this.memory = memory;
    this.priority = priority;
    this.gpu = gpu;
    this.jobName = jobName;
  }

  public TensorFlowContainerRequest(TensorFlowContainerRequest that) {
    this.virtualCores = that.virtualCores;
    this.memory = that.memory;
    this.gpu = that.gpu;
    this.priority = that.priority;
    this.jobName = that.jobName;
  }

  public int getVirtualCores() {
    return virtualCores;
  }

  public long getMemory() {
    return memory;
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

  public boolean matchesResourceRequest(Resource resource) {
    int resourceMem = (int) resource.getMemorySize();
    int resourceVCore = resource.getVirtualCores();

    if (this.memory != resourceMem || this.virtualCores != resourceVCore) {
      return false;
    }

    try {
      long numGpus = resource.getResourceValue(ResourceInformation.GPU_URI);
      return this.gpu == numGpus;
    } catch (ResourceNotFoundException e) {
      return true;
    }
  }
}
