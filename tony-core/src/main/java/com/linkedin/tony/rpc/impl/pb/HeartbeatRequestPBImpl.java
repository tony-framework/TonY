/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.HeartbeatRequest;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.HeartbeatRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.HeartbeatRequestProtoOrBuilder;


public class HeartbeatRequestPBImpl implements HeartbeatRequest {
  HeartbeatRequestProto proto = HeartbeatRequestProto.getDefaultInstance();
  HeartbeatRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private String taskId = null;

  public HeartbeatRequestPBImpl() {
    builder = HeartbeatRequestProto.newBuilder();
  }

  public HeartbeatRequestPBImpl(HeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public HeartbeatRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(this.taskId);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = HeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getTaskId() {
    HeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    this.taskId = p.getTaskId();
    return this.taskId;
  }

  @Override
  public void setTaskId(String taskId) {
    maybeInitBuilder();
    if (taskId == null) {
      builder.clearTaskId();
    }
    this.taskId = taskId;
  }
}
