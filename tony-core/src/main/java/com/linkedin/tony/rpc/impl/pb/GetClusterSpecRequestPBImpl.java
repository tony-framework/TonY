/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.GetClusterSpecRequest;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos.GetClusterSpecRequestProto;

public class GetClusterSpecRequestPBImpl implements GetClusterSpecRequest {
  private GetClusterSpecRequestProto proto = GetClusterSpecRequestProto.getDefaultInstance();
  private GetClusterSpecRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private String taskId;

  public GetClusterSpecRequestPBImpl() {
        builder = GetClusterSpecRequestProto.newBuilder();
    }

  public GetClusterSpecRequestPBImpl(GetClusterSpecRequestProto proto) {
    this.proto = proto;
    viaProto = true;
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

  public GetClusterSpecRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterSpecRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getTaskId() {
    YarnTonyClusterProtos.GetClusterSpecRequestProtoOrBuilder p = viaProto ? proto : builder;
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
