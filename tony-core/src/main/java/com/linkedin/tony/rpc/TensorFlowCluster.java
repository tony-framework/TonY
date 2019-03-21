/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSelector;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.ipc.ProtocolInfo;

import java.io.IOException;

@TokenInfo(ClientToAMTokenSelector.class)
@ProtocolInfo(
    protocolName = "com.linkedin.tony.rpc.TensorFlowCluster",
    protocolVersion = 1)
public interface TensorFlowCluster extends VersionedProtocol {
  long versionID = 1L;

  GetTaskInfosResponse getTaskInfos(GetTaskInfosRequest request) throws IOException, YarnException;

  GetClusterSpecResponse getClusterSpec(GetClusterSpecRequest request)
      throws YarnException, IOException;

  RegisterWorkerSpecResponse registerWorkerSpec(RegisterWorkerSpecRequest request)
      throws YarnException, IOException;
  RegisterTensorBoardUrlResponse registerTensorBoardUrl(RegisterTensorBoardUrlRequest request)
      throws Exception;
  RegisterExecutionResultResponse registerExecutionResult(RegisterExecutionResultRequest request) throws Exception;
  Empty finishApplication(Empty request) throws YarnException, IOException;

  HeartbeatResponse taskExecutorHeartbeat(HeartbeatRequest request) throws YarnException, IOException;

}
