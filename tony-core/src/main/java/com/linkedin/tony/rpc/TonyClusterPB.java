/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import org.apache.hadoop.ipc.ProtocolInfo;
import com.linkedin.tony.rpc.proto.TonyCluster.TonyClusterService;

@ProtocolInfo(
  protocolName = "com.linkedin.tony.rpc.TonyCluster",
  protocolVersion = 1)
public interface TonyClusterPB extends TonyClusterService.BlockingInterface {
}
