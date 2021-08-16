/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.Empty;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos.EmptyProto;


public class EmptyPBImpl implements Empty {
  private EmptyProto proto = EmptyProto.getDefaultInstance();
  private EmptyProto.Builder builder = null;
  private boolean viaProto = false;

  private boolean rebuild = false;

  public EmptyPBImpl() {
    builder = EmptyProto.newBuilder();
  }

  public EmptyPBImpl(EmptyProto proto) {
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

  public EmptyProto getProto() {
    if (rebuild) {
      mergeLocalToProto();
    }
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = EmptyProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
