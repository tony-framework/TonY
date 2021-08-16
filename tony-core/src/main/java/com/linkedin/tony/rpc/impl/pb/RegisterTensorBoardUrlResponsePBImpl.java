/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterTensorBoardUrlResponse;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos.RegisterTensorBoardUrlResponseProto;


public class RegisterTensorBoardUrlResponsePBImpl implements RegisterTensorBoardUrlResponse {
  private RegisterTensorBoardUrlResponseProto proto = RegisterTensorBoardUrlResponseProto.getDefaultInstance();
  private RegisterTensorBoardUrlResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private String spec = null;

  public RegisterTensorBoardUrlResponsePBImpl() {
    builder = RegisterTensorBoardUrlResponseProto.newBuilder();
  }

  public RegisterTensorBoardUrlResponsePBImpl(RegisterTensorBoardUrlResponseProto proto) {
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

  public RegisterTensorBoardUrlResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterTensorBoardUrlResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  @Override
  public String getSpec() {
    YarnTonyClusterProtos.RegisterTensorBoardUrlResponseProtoOrBuilder p = viaProto ? proto : builder;
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
