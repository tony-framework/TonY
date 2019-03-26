/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos;
import com.linkedin.tony.util.ProtoUtils;
import com.linkedin.tony.rpc.GetTaskInfosResponse;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskInfosResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskInfosResponseProtoOrBuilder;
import java.util.Set;
import java.util.stream.Collectors;


public class GetTaskInfosResponsePBImpl implements GetTaskInfosResponse {
  GetTaskInfosResponseProto proto = GetTaskInfosResponseProto.getDefaultInstance();
  GetTaskInfosResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private Set<TaskInfo> _taskInfos = null;

  public GetTaskInfosResponsePBImpl() {
    builder = YarnTensorFlowClusterProtos.GetTaskInfosResponseProto.newBuilder();
  }

  public GetTaskInfosResponsePBImpl(GetTaskInfosResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnTensorFlowClusterProtos.GetTaskInfosResponseProto getProto() {
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
      builder = GetTaskInfosResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Set<TaskInfo> getTaskInfos() {
    GetTaskInfosResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this._taskInfos != null) {
      return this._taskInfos;
    }
    return p.getTaskInfosList().stream().map(ProtoUtils::taskInfoProtoToTaskInfo).collect(Collectors.toSet());
  }

  @Override
  public void setTaskInfos(Set<TaskInfo> taskInfos) {
    maybeInitBuilder();
    this._taskInfos = taskInfos;
    builder.clearTaskInfos();
    builder.addAllTaskInfos(taskInfos.stream().map(ProtoUtils::taskInfoToTaskInfoProto)
        .collect(Collectors.toList()));
  }
}
