/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.util.ProtoUtils;
import com.linkedin.tony.rpc.GetTaskUrlsResponse;
import com.linkedin.tony.rpc.TaskUrl;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskUrlsResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskUrlsResponseProtoOrBuilder;
import java.util.Set;
import java.util.stream.Collectors;


public class GetTaskUrlsResponsePBImpl implements GetTaskUrlsResponse {
  GetTaskUrlsResponseProto proto = GetTaskUrlsResponseProto.getDefaultInstance();
  GetTaskUrlsResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private Set<TaskUrl> _taskUrls = null;

  public GetTaskUrlsResponsePBImpl() {
    builder = GetTaskUrlsResponseProto.newBuilder();
  }

  public GetTaskUrlsResponsePBImpl(GetTaskUrlsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetTaskUrlsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskUrlsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Set<TaskUrl> getTaskUrls() {
    GetTaskUrlsResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this._taskUrls != null) {
      return this._taskUrls;
    }
    return p.getTaskUrlsList().stream().map(ProtoUtils::taskUrlProtoToTaskUrl).collect(Collectors.toSet());
  }

  @Override
  public void setTaskUrls(Set<TaskUrl> taskUrls) {
    maybeInitBuilder();
    this._taskUrls = taskUrls;
    builder.clearTaskUrls();
    builder.addAllTaskUrls(taskUrls.stream().map(ProtoUtils::taskUrlToTaskUrlProto)
        .collect(Collectors.toList()));
  }
}
