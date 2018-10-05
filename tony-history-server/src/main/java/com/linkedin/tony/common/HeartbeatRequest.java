/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class HeartbeatRequest implements Writable {
  private THSContainerStatus THSContainerStatus;
  private BooleanWritable interResultSavedStatus;
  private String progressLog;
  private String containersStartTime;
  private String containersFinishTime;

  public HeartbeatRequest() {
    THSContainerStatus = THSContainerStatus.UNDEFINED;
    interResultSavedStatus = new BooleanWritable(false);
    progressLog = "";
    containersStartTime = "";
    containersFinishTime = "";
  }

  public void setXLearningContainerStatus(THSContainerStatus THSContainerStatus) {
    this.THSContainerStatus = THSContainerStatus;
  }

  public THSContainerStatus getXLearningContainerStatus() {
    return this.THSContainerStatus;
  }

  public void setInnerModelSavedStatus(Boolean savedStatus) {
    this.interResultSavedStatus.set(savedStatus);
  }

  public Boolean getInnerModelSavedStatus() {
    return this.interResultSavedStatus.get();
  }

  public void setProgressLog(String xlearningProgress) {
    this.progressLog = xlearningProgress;
  }

  public String getProgressLog() {
    return this.progressLog;
  }

  public void setContainersStartTime(String startTime) {
    this.containersStartTime = startTime;
  }

  public String getContainersStartTime() {
    return this.containersStartTime;
  }

  public void setContainersFinishTime(String finishTime) {
    this.containersFinishTime = finishTime;
  }

  public String getContainersFinishTime() {
    return this.containersFinishTime;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeEnum(dataOutput, this.THSContainerStatus);
    interResultSavedStatus.write(dataOutput);
    Text.writeString(dataOutput, this.progressLog);
    Text.writeString(dataOutput, this.containersStartTime);
    Text.writeString(dataOutput, this.containersFinishTime);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.THSContainerStatus = WritableUtils.readEnum(dataInput, THSContainerStatus.class);
    interResultSavedStatus.readFields(dataInput);
    this.progressLog = Text.readString(dataInput);
    this.containersStartTime = Text.readString(dataInput);
    this.containersFinishTime = Text.readString(dataInput);
  }

}
