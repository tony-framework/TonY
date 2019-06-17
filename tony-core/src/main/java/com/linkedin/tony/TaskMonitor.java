/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.tony.rpc.MetricWritable;
import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.impl.MetricsWritable;
import com.linkedin.tony.util.gpu.GpuDeviceInformation;
import com.linkedin.tony.util.gpu.GpuDiscoverer;
import com.linkedin.tony.util.gpu.GpuInfoException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;


/**
 * Monitors a task and reports metrics to the AM.
 */
class TaskMonitor implements Runnable {
  private static final Log LOG = LogFactory.getLog(TaskMonitor.class);

  private String taskType;
  private int taskIndex;
  private MetricsRpc metricsRpcClient;
  private ResourceCalculatorProcessTree resourceCalculator;
  private GpuDiscoverer gpuDiscoverer;

  public static final List<String> METRICS_TO_COLLECT = ImmutableList.of(Constants.MAX_MEMORY_BYTES,
      Constants.AVG_MEMORY_BYTES, Constants.MAX_GPU_UTILIZATION, Constants.AVG_GPU_UTILIZATION,
      Constants.MAX_GPU_FB_MEMORY_USAGE, Constants.AVG_GPU_FB_MEMORY_USAGE,
      Constants.MAX_GPU_MAIN_MEMORY_USAGE, Constants.AVG_GPU_MAIN_MEMORY_USAGE);

  public static final int MAX_MEMORY_BYTES_INDEX = 0;
  public static final int AVG_MEMORY_BYTES_INDEX = 1;
  public static final int MAX_GPU_UTILIZATION_INDEX = 2;
  public static final int AVG_GPU_UTILIZATION_INDEX = 3;
  public static final int MAX_GPU_FB_MEMORY_USAGE_INDEX = 4;
  public static final int AVG_GPU_FB_MEMORY_USAGE_INDEX = 5;
  public static final int MAX_GPU_MAIN_MEMORY_USAGE_INDEX = 6;
  public static final int AVG_GPU_MAIN_MEMORY_USAGE_INDEX = 7;

  private Boolean isGpuMachine;

  private MetricsWritable metrics = new MetricsWritable(METRICS_TO_COLLECT.size());

  @VisibleForTesting
  protected int numRefreshes = 0;

  TaskMonitor(String taskType, int taskIndex,
      Configuration yarnConf, Configuration tonyConf, MetricsRpc metricsRpcClient) {
    this.taskType = taskType;
    this.taskIndex = taskIndex;

    initMetrics();
    this.metricsRpcClient = metricsRpcClient;

    String pid = System.getenv(Constants.JVM_PID);
    LOG.info("Task pid is: " + pid);

    isGpuMachine = checkIsGpuMachine(tonyConf);

    this.resourceCalculator = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(pid, null, yarnConf);
    if (isGpuMachine) {
      this.gpuDiscoverer = GpuDiscoverer.getInstance();
      this.gpuDiscoverer.initialize(yarnConf);
    }
  }

  @VisibleForTesting
  void initMetrics() {
    for (int i = 0; i < METRICS_TO_COLLECT.size(); i++) {
      metrics.setMetric(i, new MetricWritable(METRICS_TO_COLLECT.get(i), -1d));
    }
  }

  private boolean checkIsGpuMachine(Configuration conf) {
    int numWorkerGpus = conf.getInt(
        TonyConfigurationKeys.getResourceKey(taskType, "gpus"), 0);
    LOG.info("Number of GPUs requested for " + taskType + ": " + numWorkerGpus);
    return numWorkerGpus > 0;
  }

  @Override
  public void run() {
    refreshMetrics();
    try {
      metricsRpcClient.updateMetrics(taskType, taskIndex, metrics);
    } catch (Exception e) {
      LOG.error("Encountered exception updating metrics", e);
    }
  }

  private void refreshMetrics() {
    refreshMemoryBytesMetrics();
    if (isGpuMachine) {
      refreshGPUMetrics();
    }
    numRefreshes++;
  }

  private void refreshMemoryBytesMetrics() {
    resourceCalculator.updateProcessTree();
    double memoryBytes = resourceCalculator.getRssMemorySize();
    setMaxMetrics(MAX_MEMORY_BYTES_INDEX, memoryBytes);
    setAvgMetrics(AVG_MEMORY_BYTES_INDEX, memoryBytes);
  }

  private void refreshGPUMetrics() {
    try {
      GpuDeviceInformation gpuInfo = gpuDiscoverer.getGpuDeviceInformation();

      double maxGpuUtilization = gpuInfo.getGpus().stream()
          .mapToDouble(x -> x.getGpuUtilizations().getOverallGpuUtilization())
          .max()
          .getAsDouble();
      double avgGpuUtilization = gpuInfo.getGpus().stream()
          .mapToDouble(x -> x.getGpuUtilizations().getOverallGpuUtilization())
          .average()
          .getAsDouble();
      double maxGpuFBMemoryUsage = gpuInfo.getGpus().stream()
          .mapToDouble((x ->
              ((double) x.getGpuFBMemoryUsage().getUsedMemoryMiB() / x.getGpuFBMemoryUsage().getTotalMemoryMiB() * 100)))
          .max()
          .getAsDouble();
      double avgGpuFBMemoryUsage = gpuInfo.getGpus().stream()
          .mapToDouble((x ->
              ((double) x.getGpuFBMemoryUsage().getUsedMemoryMiB() / x.getGpuFBMemoryUsage().getTotalMemoryMiB() * 100)))
          .average()
          .getAsDouble();
      double maxGpuMainMemoryUsage = gpuInfo.getGpus().stream()
          .mapToDouble((x ->
              ((double) x.getGpuMainMemoryUsage().getUsedMemoryMiB() / x.getGpuMainMemoryUsage().getTotalMemoryMiB() * 100)))
          .max()
          .getAsDouble();
      double avgGpuMainMemoryUsage = gpuInfo.getGpus().stream()
          .mapToDouble((x ->
              ((double) x.getGpuMainMemoryUsage().getUsedMemoryMiB() / x.getGpuMainMemoryUsage().getTotalMemoryMiB() * 100)))
          .average()
          .getAsDouble();

      setMaxMetrics(MAX_GPU_UTILIZATION_INDEX, maxGpuUtilization);
      setAvgMetrics(AVG_GPU_UTILIZATION_INDEX, avgGpuUtilization);
      setMaxMetrics(MAX_GPU_FB_MEMORY_USAGE_INDEX, maxGpuFBMemoryUsage);
      setAvgMetrics(AVG_GPU_FB_MEMORY_USAGE_INDEX, avgGpuFBMemoryUsage);
      setMaxMetrics(MAX_GPU_MAIN_MEMORY_USAGE_INDEX, maxGpuMainMemoryUsage);
      setAvgMetrics(AVG_GPU_MAIN_MEMORY_USAGE_INDEX, avgGpuMainMemoryUsage);
    } catch (GpuInfoException e) {
      // Follow YARN's GPUDiscoverer mechanism of capping number of gpu metrics query
      if (gpuDiscoverer.getNumOfErrorExecutionSinceLastSucceed() >= Constants.MAX_REPEATED_GPU_ERROR_ALLOWED) {
        LOG.warn("Failed to collect GPU metrics", e);
        isGpuMachine = false;
        return;
      }

      LOG.warn("Failed to collect GPU metrics at " + (numRefreshes + 1) + "th run, retry...", e);
    }
  }

  @VisibleForTesting
  void setAvgMetrics(int metricIndex, double newMetricValue) {
    MetricWritable metric = metrics.getMetric(metricIndex);
    metric.setValue((metric.getValue() * numRefreshes + newMetricValue) / (numRefreshes + 1));
    metrics.setMetric(metricIndex, metric);
  }

  @VisibleForTesting
  void setMaxMetrics(int metricIndex, double newMetricValue) {
    MetricWritable metric = metrics.getMetric(metricIndex);
    if (newMetricValue > metric.getValue()) {
      metric.setValue(newMetricValue);
      metrics.setMetric(metricIndex, metric);
    }
  }

  @VisibleForTesting
  MetricsWritable getMetrics() {
    return this.metrics;
  }
}
