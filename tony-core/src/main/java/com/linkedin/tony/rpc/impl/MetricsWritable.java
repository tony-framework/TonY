/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl;

import com.linkedin.tony.events.Metric;
import com.linkedin.tony.rpc.MetricWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.io.Writable;


/**
 * For serializing an array of {@link MetricWritable} over the wire.
 */
public class MetricsWritable implements Writable {
  private MetricWritable[] metrics;

  // Required for serialization
  public MetricsWritable() { }

  public MetricsWritable(int numMetrics) {
    this.metrics = new MetricWritable[numMetrics];
  }

  public MetricWritable getMetric(int index) {
    return metrics[index];
  }

  public void setMetric(int index, MetricWritable metric) {
    metrics[index] = metric;
  }

  public List<Metric> getMetricsAsList() {
    return Arrays.stream(metrics).map(metric -> new Metric(metric.getName(), metric.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(metrics.length);
    for (MetricWritable metric : metrics) {
      metric.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    metrics = new MetricWritable[in.readInt()];
    for (int i = 0; i < metrics.length; i++) {
      MetricWritable metric = new MetricWritable();
      metric.readFields(in);
      metrics[i] = metric;
    }
  }
}
