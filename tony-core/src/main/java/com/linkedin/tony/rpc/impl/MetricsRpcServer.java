/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl;

import com.linkedin.tony.events.Metric;
import com.linkedin.tony.rpc.MetricsRpc;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.ProtocolSignature;


/**
 * Stores metrics and handles metric updates for all tasks.
 */
public class MetricsRpcServer implements MetricsRpc {
  private static final Log LOG = LogFactory.getLog(MetricsRpcServer.class);

  private Map<String, Map<Integer, MetricsWritable>> metricsMap = new HashMap<>();

  public List<Metric> getMetrics(String taskType, int taskIndex) {
    if (!metricsMap.containsKey(taskType) || !metricsMap.get(taskType).containsKey(taskIndex)) {
      LOG.warn("No metrics for " + taskType + " " + taskIndex + "!");
      return Collections.EMPTY_LIST;
    }
    return metricsMap.get(taskType).get(taskIndex).getMetricsAsList();
  }

  /**
   * Replaces the metrics stored for {@code taskType} {@code taskIndex} with {@code metrics}.
   */
  @Override
  public void updateMetrics(String taskType, int taskIndex, MetricsWritable metrics) {
    if (!metricsMap.containsKey(taskType)) {
      metricsMap.put(taskType, new HashMap<>());
    }
    metricsMap.get(taskType).put(taskIndex, metrics);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
      throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
  }
}
