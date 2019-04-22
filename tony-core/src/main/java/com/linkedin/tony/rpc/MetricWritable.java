/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


/**
 * For serializing a {@link com.linkedin.tony.events.Metric} over the wire.
 */
public class MetricWritable implements Writable {
  private String name;
  private double value;

  public MetricWritable() { }

  public MetricWritable(String name, double value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(name);
    out.writeDouble(value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    name = in.readUTF();
    value = in.readDouble();
  }
}
