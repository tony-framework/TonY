/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;


import com.linkedin.tony.rpc.RegisterWorkerSpecResponse;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterWorkerSpecResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterWorkerSpecResponseProtoOrBuilder;

public class RegisterWorkerSpecResponsePBImpl implements RegisterWorkerSpecResponse {
  private RegisterWorkerSpecResponseProto proto = RegisterWorkerSpecResponseProto.getDefaultInstance();
  private RegisterWorkerSpecResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private String spec = null;

  public RegisterWorkerSpecResponsePBImpl() {
    builder = RegisterWorkerSpecResponseProto.newBuilder();
  }

  public RegisterWorkerSpecResponsePBImpl(RegisterWorkerSpecResponseProto proto) {
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

  public RegisterWorkerSpecResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterWorkerSpecResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  @Override
  public String getSpec() {
    RegisterWorkerSpecResponseProtoOrBuilder p = viaProto ? proto : builder;
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
