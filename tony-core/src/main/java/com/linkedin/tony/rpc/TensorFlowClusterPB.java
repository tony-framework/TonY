/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import org.apache.hadoop.ipc.ProtocolInfo;
import com.linkedin.tony.rpc.proto.TensorFlowCluster.TensorFlowClusterService;

@ProtocolInfo(
  protocolName = "com.linkedin.tony.rpc.TensorFlowCluster",
  protocolVersion = 1)
public interface TensorFlowClusterPB extends TensorFlowClusterService.BlockingInterface {
}
