/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.GetClusterSpecResponse;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetClusterSpecResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetClusterSpecResponseProtoOrBuilder;

public class GetClusterSpecResponsePBImpl implements GetClusterSpecResponse {
  GetClusterSpecResponseProto proto = GetClusterSpecResponseProto.getDefaultInstance();
  GetClusterSpecResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private String clusterSpec = null;

  public GetClusterSpecResponsePBImpl() {
      builder = GetClusterSpecResponseProto.newBuilder();
  }

  public GetClusterSpecResponsePBImpl(GetClusterSpecResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetClusterSpecResponseProto getProto() {
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
    if (this.clusterSpec != null) {
      builder.setClusterSpec(this.clusterSpec);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterSpecResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getClusterSpec() {
    GetClusterSpecResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.clusterSpec != null) {
      return this.clusterSpec;
    }
    if (!p.hasClusterSpec()) {
      return null;
    }
    this.clusterSpec = p.getClusterSpec();
    return this.clusterSpec;
  }

  @Override
  public void setClusterSpec(String clusterSpec) {
    maybeInitBuilder();
    if (clusterSpec == null) {
      builder.clearClusterSpec();
    }
    this.clusterSpec = clusterSpec;
  }
}
