/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.util.ProtoUtils;
import com.linkedin.tony.rpc.GetTaskInfosResponse;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskUrlsResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskUrlsResponseProtoOrBuilder;
import java.util.Set;
import java.util.stream.Collectors;


public class GetTaskInfosResponsePBImpl implements GetTaskInfosResponse {
  GetTaskUrlsResponseProto proto = GetTaskUrlsResponseProto.getDefaultInstance();
  GetTaskUrlsResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private Set<TaskInfo> _taskInfos = null;

  public GetTaskInfosResponsePBImpl() {
    builder = GetTaskUrlsResponseProto.newBuilder();
  }

  public GetTaskInfosResponsePBImpl(GetTaskUrlsResponseProto proto) {
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
  public Set<TaskInfo> getTaskInfos() {
    GetTaskUrlsResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this._taskInfos != null) {
      return this._taskInfos;
    }
    return p.getTaskUrlsList().stream().map(ProtoUtils::taskUrlProtoToTaskUrl).collect(Collectors.toSet());
  }

  @Override
  public void setTaskInfos(Set<TaskInfo> taskInfos) {
    maybeInitBuilder();
    this._taskInfos = taskInfos;
    builder.clearTaskUrls();
    builder.addAllTaskUrls(taskInfos.stream().map(ProtoUtils::taskUrlToTaskUrlProto)
        .collect(Collectors.toList()));
  }
}
