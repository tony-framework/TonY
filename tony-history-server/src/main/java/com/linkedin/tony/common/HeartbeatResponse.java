/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HeartbeatResponse implements Writable {

  private BooleanWritable isTrainingCompleted;
  private LongWritable interResultTimeStamp;

  private static final Log LOG = LogFactory.getLog(HeartbeatResponse.class);

  public HeartbeatResponse() {
    isTrainingCompleted = new BooleanWritable(false);
    interResultTimeStamp = new LongWritable(Long.MIN_VALUE);
  }

  public HeartbeatResponse(Boolean isTrainingCompleted, Long timeStamp) {
    this.isTrainingCompleted = new BooleanWritable(isTrainingCompleted);
    this.interResultTimeStamp = new LongWritable(timeStamp);
  }

  public Long getInnerModelTimeStamp() {
    return interResultTimeStamp.get();
  }

  public Boolean getIsXLearningTrainCompleted() {
    return this.isTrainingCompleted.get();
  }

  @Override
  public void write(DataOutput dataOutput) {
    try {
      isTrainingCompleted.write(dataOutput);
      interResultTimeStamp.write(dataOutput);
    } catch (IOException e) {
      LOG.error("containerStatus write error: " + e);
    }
  }

  @Override
  public void readFields(DataInput dataInput) {
    try {
      isTrainingCompleted.readFields(dataInput);
      interResultTimeStamp.readFields(dataInput);
    } catch (IOException e) {
      LOG.error("containerStatus read error:" + e);
    }
  }
}
