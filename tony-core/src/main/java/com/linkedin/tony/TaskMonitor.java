package com.linkedin.tony;

import com.linkedin.tony.rpc.MetricWritable;
import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.impl.MetricsWritable;
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

  private MetricWritable[] metrics = new MetricWritable[]{
      new MetricWritable(Constants.MAX_MEMORY_BYTES, -1d)
  };
  private MetricsWritable metricsWritable = new MetricsWritable(metrics);

  public static final int MAX_MEMORY_BYTES_INDEX = 0;

  TaskMonitor(String taskType, int taskIndex, Configuration conf, MetricsRpc metricsRpcClient) {
    this.taskType = taskType;
    this.taskIndex = taskIndex;

    this.metricsRpcClient = metricsRpcClient;

    String pid = System.getenv(Constants.JVM_PID);
    LOG.info("Task pid is: " + pid);
    this.resourceCalculator = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(pid, null, conf);

  }

  @Override
  public void run() {
    refreshMetrics();
    try {
      metricsRpcClient.updateMetrics(taskType, taskIndex, metricsWritable);
    } catch (Exception e) {
      LOG.error("Encountered exception updating metrics", e);
    }
  }

  private void refreshMetrics() {
    resourceCalculator.updateProcessTree();
    refreshMaxMemoryBytes();
  }

  private void refreshMaxMemoryBytes() {
    double memoryBytes = resourceCalculator.getRssMemorySize();
    if (memoryBytes > metrics[MAX_MEMORY_BYTES_INDEX].getValue()) {
      metrics[MAX_MEMORY_BYTES_INDEX].setValue(memoryBytes);
    }
  }
}
