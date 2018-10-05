/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Message implements Writable {
  private LogType logType;
  private String message;

  public Message() {
    this.logType = LogType.STDERR;
    this.message = "";
  }

  public Message(LogType logType, String message) {
    this.logType = logType;
    this.message = message;
  }

  public LogType getLogType() {
    return logType;
  }

  public String getMessage() {
    return message;
  }


  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeEnum(dataOutput, this.logType);
    Text.writeString(dataOutput, message);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.logType = WritableUtils.readEnum(dataInput, LogType.class);
    this.message = Text.readString(dataInput);
  }
}
