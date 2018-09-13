/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterWorkerSpecRequest;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterWorkerSpecRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterWorkerSpecRequestProtoOrBuilder;


public class RegisterWorkerSpecRequestPBImpl implements RegisterWorkerSpecRequest {
  private RegisterWorkerSpecRequestProto proto = RegisterWorkerSpecRequestProto.getDefaultInstance();
  private RegisterWorkerSpecRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private String worker = null;
  private String spec = null;

  public RegisterWorkerSpecRequestPBImpl() {
    builder = RegisterWorkerSpecRequestProto.newBuilder();
  }

  public RegisterWorkerSpecRequestPBImpl(RegisterWorkerSpecRequestProto proto) {
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
    if (this.worker != null) {
      builder.setWorker(this.worker);
    }
    if (this.spec != null) {
      builder.setSpec(this.spec);
    }
  }

  public RegisterWorkerSpecRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterWorkerSpecRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getWorker() {
    RegisterWorkerSpecRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.worker != null) {
      return this.worker;
    }
    if (!p.hasWorker()) {
      return null;
    }
    this.worker = p.getWorker();
    return this.worker;
  }

  @Override
  public void setWorker(String worker) {
    maybeInitBuilder();
    if (worker == null) {
      builder.clearWorker();
    }
    this.worker = worker;
  }

  @Override
  public String getSpec() {
    RegisterWorkerSpecRequestProtoOrBuilder p = viaProto ? proto : builder;
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
