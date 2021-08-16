/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;


import com.linkedin.tony.rpc.GetTaskInfosRequest;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos.GetTaskInfosRequestProto;


public class GetTaskInfosRequestPBImpl implements GetTaskInfosRequest {
  private YarnTonyClusterProtos.GetTaskInfosRequestProto proto = YarnTonyClusterProtos.GetTaskInfosRequestProto.getDefaultInstance();
  private YarnTonyClusterProtos.GetTaskInfosRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private boolean rebuild = false;

  public GetTaskInfosRequestPBImpl() {
        builder = GetTaskInfosRequestProto.newBuilder();
    }

  public GetTaskInfosRequestPBImpl(YarnTonyClusterProtos.GetTaskInfosRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  public YarnTonyClusterProtos.GetTaskInfosRequestProto getProto() {
     if (rebuild) {
       mergeLocalToProto();
     }
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnTonyClusterProtos.GetTaskInfosRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
