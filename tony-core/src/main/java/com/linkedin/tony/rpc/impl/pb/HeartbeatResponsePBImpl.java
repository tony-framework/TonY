/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.HeartbeatResponse;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.HeartbeatResponseProto;


public class HeartbeatResponsePBImpl implements HeartbeatResponse {
  private HeartbeatResponseProto proto = HeartbeatResponseProto.getDefaultInstance();
  private HeartbeatResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private boolean rebuild = false;

  public HeartbeatResponsePBImpl() {
    builder = HeartbeatResponseProto.newBuilder();
  }

  public HeartbeatResponsePBImpl(HeartbeatResponseProto proto) {
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

  public HeartbeatResponseProto getProto() {
    if (rebuild) {
      mergeLocalToProto();
    }
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = HeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
