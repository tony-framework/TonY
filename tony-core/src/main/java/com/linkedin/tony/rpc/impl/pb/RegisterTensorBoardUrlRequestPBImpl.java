/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterTensorBoardUrlRequest;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterTensorBoardUrlRequestProto;


public class RegisterTensorBoardUrlRequestPBImpl implements RegisterTensorBoardUrlRequest {
  private RegisterTensorBoardUrlRequestProto proto = RegisterTensorBoardUrlRequestProto.getDefaultInstance();
  private RegisterTensorBoardUrlRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private String spec = null;

  public RegisterTensorBoardUrlRequestPBImpl() {
    builder = RegisterTensorBoardUrlRequestProto.newBuilder();
  }

  public RegisterTensorBoardUrlRequestPBImpl(RegisterTensorBoardUrlRequestProto proto) {
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
    if (this.spec != null) {
      builder.setSpec(this.spec);
    }
  }

  public RegisterTensorBoardUrlRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterTensorBoardUrlRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getSpec() {
    YarnTensorFlowClusterProtos.RegisterTensorBoardUrlRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.spec != null) {
      return this.spec;
    }
    if (!p.hasSpec()) {
      return null;
    }
    this.spec = p.getSpec();
    return this.spec;
  }

  @Override
  public void setSpec(String spec) {
    maybeInitBuilder();
    if (spec == null) {
      builder.clearSpec();
    }
    this.spec = spec;
  }
}
