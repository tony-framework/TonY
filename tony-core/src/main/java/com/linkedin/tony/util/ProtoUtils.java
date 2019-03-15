/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskUrlsResponseProto.TaskUrlProto;


public class ProtoUtils {
  public static TaskInfo taskUrlProtoToTaskUrl(TaskUrlProto taskUrlProto) {
    return new TaskInfo(taskUrlProto.getName(), taskUrlProto.getIndex(), taskUrlProto.getUrl());
  }

  public static TaskUrlProto taskUrlToTaskUrlProto(TaskInfo taskInfo) {
    return TaskUrlProto.newBuilder().setName(taskInfo.getName()).setIndex(taskInfo.getIndex())
        .setUrl(taskInfo.getUrl()).build();
  }

  private ProtoUtils() { }
}
