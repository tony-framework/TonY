/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterExecutionResultResponse;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterExecutionResultResponseProto;


public class RegisterExecutionResultResponsePBImpl implements RegisterExecutionResultResponse {
  private RegisterExecutionResultResponseProto proto = RegisterExecutionResultResponseProto.getDefaultInstance();
  private RegisterExecutionResultResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private String message = null;

  public RegisterExecutionResultResponsePBImpl() {
    builder = RegisterExecutionResultResponseProto.newBuilder();
  }

  public RegisterExecutionResultResponsePBImpl(RegisterExecutionResultResponseProto proto) {
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
    if (this.message != null) {
      builder.setMessage(this.message);
    }
  }

  public RegisterExecutionResultResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterExecutionResultResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  @Override
  public String getMessage() {
    YarnTensorFlowClusterProtos.RegisterExecutionResultResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.message != null) {
      return this.message;
    }
    if (!p.hasMessage()) {
      return null;
    }
    this.message = p.getMessage();
    return this.message;
  }

  @Override
  public void setMessage(String message) {
    maybeInitBuilder();
    if (message == null) {
      builder.clearMessage();
    }
    this.message = message;
  }
}
