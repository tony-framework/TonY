/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.impl.MetricsWritable;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestTaskMonitor {
  @Mock
  Configuration yarnConf = mock(Configuration.class);

  @Mock
  Configuration tonyConf = mock(Configuration.class);

  @Mock
  MetricsRpc metricsRpcClient;

  @Mock
  private TaskMonitor taskMonitor = mock(TaskMonitor.class);

  @BeforeTest
  public void setupTaskMonitor() {
    when(yarnConf.get(TonyConfigurationKeys.GPU_PATH_TO_EXEC, TonyConfigurationKeys.DEFAULT_GPU_PATH_TO_EXEC))
        .thenReturn(TonyConfigurationKeys.DEFAULT_GPU_PATH_TO_EXEC);
    when(tonyConf.getInt(TonyConfigurationKeys.getResourceKey("worker", "gpus"), 0))
        .thenReturn(1);
    taskMonitor = new TaskMonitor("worker", 0, yarnConf, tonyConf, metricsRpcClient);
    taskMonitor.initMetrics();
  }

  @Test
  public void testSetAvgMetrics() {
    taskMonitor.setAvgMetrics(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX, 0.1);
    taskMonitor.numRefreshes++;
    taskMonitor.setAvgMetrics(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX, 0.3);
    taskMonitor.numRefreshes++;
    taskMonitor.setAvgMetrics(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX, 0.5);
    MetricsWritable metrics = taskMonitor.getMetrics();
    Assert.assertEquals(metrics.getMetric(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX).getValue(), 0.3);
  }

  @Test
  public void testSetMaxMetrics() {
    taskMonitor.setMaxMetrics(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX, 0.1);
    taskMonitor.numRefreshes++;
    taskMonitor.setMaxMetrics(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX, 0.4);
    taskMonitor.numRefreshes++;
    taskMonitor.setMaxMetrics(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX, 0.2);
    MetricsWritable metrics = taskMonitor.getMetrics();
    Assert.assertEquals(metrics.getMetric(TaskMonitor.AVG_GPU_FB_MEMORY_USAGE_INDEX).getValue(), 0.4);
  }
}
