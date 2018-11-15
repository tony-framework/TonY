/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.rpc.TaskUrl;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskUrlsResponseProto.TaskUrlProto;


public class ProtoUtils {
  public static TaskUrl taskUrlProtoToTaskUrl(TaskUrlProto taskUrlProto) {
    return new TaskUrl(taskUrlProto.getName(), taskUrlProto.getIndex(), taskUrlProto.getUrl());
  }

  public static TaskUrlProto taskUrlToTaskUrlProto(TaskUrl taskUrl) {
    return TaskUrlProto.newBuilder().setName(taskUrl.getName()).setIndex(taskUrl.getIndex())
        .setUrl(taskUrl.getUrl()).build();
  }

  private ProtoUtils() { }
}
