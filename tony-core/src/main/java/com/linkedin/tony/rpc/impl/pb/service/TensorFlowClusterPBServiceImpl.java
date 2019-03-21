/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.linkedin.tony.rpc.Empty;
import com.linkedin.tony.rpc.GetClusterSpecResponse;
import com.linkedin.tony.rpc.GetTaskInfosResponse;
import com.linkedin.tony.rpc.HeartbeatResponse;
import com.linkedin.tony.rpc.RegisterExecutionResultResponse;
import com.linkedin.tony.rpc.RegisterTensorBoardUrlResponse;
import com.linkedin.tony.rpc.RegisterWorkerSpecResponse;
import com.linkedin.tony.rpc.TensorFlowCluster;
import com.linkedin.tony.rpc.TensorFlowClusterPB;
import com.linkedin.tony.rpc.impl.pb.EmptyPBImpl;
import com.linkedin.tony.rpc.impl.pb.GetClusterSpecRequestPBImpl;
import com.linkedin.tony.rpc.impl.pb.GetClusterSpecResponsePBImpl;
import com.linkedin.tony.rpc.impl.pb.GetTaskInfosRequestPBImpl;
import com.linkedin.tony.rpc.impl.pb.GetTaskInfosResponsePBImpl;
import com.linkedin.tony.rpc.impl.pb.HeartbeatRequestPBImpl;
import com.linkedin.tony.rpc.impl.pb.HeartbeatResponsePBImpl;
import com.linkedin.tony.rpc.impl.pb.RegisterExecutionResultRequestPBImpl;
import com.linkedin.tony.rpc.impl.pb.RegisterExecutionResultResponsePBImpl;
import com.linkedin.tony.rpc.impl.pb.RegisterTensorBoardUrlRequestPBImpl;
import com.linkedin.tony.rpc.impl.pb.RegisterTensorBoardUrlResponsePBImpl;
import com.linkedin.tony.rpc.impl.pb.RegisterWorkerSpecRequestPBImpl;
import com.linkedin.tony.rpc.impl.pb.RegisterWorkerSpecResponsePBImpl;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.EmptyProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetClusterSpecRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetClusterSpecResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.GetTaskInfosRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterExecutionResultRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterExecutionResultResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterTensorBoardUrlRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterTensorBoardUrlResponseProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterWorkerSpecRequestProto;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterWorkerSpecResponseProto;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class TensorFlowClusterPBServiceImpl implements TensorFlowClusterPB {
  private TensorFlowCluster real;

  public TensorFlowClusterPBServiceImpl(TensorFlowCluster impl) {
    this.real = impl;
  }

  @Override
  public YarnTensorFlowClusterProtos.GetTaskInfosResponseProto getTaskInfos(RpcController controller,
                                                                           GetTaskInfosRequestProto proto) throws ServiceException {
    GetTaskInfosRequestPBImpl request = new GetTaskInfosRequestPBImpl(proto);
    try {
      GetTaskInfosResponse response = real.getTaskInfos(request);
      return ((GetTaskInfosResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetClusterSpecResponseProto getClusterSpec(RpcController controller,
                                                    GetClusterSpecRequestProto proto) throws ServiceException {
    GetClusterSpecRequestPBImpl request = new GetClusterSpecRequestPBImpl(proto);
    try {
      GetClusterSpecResponse response = real.getClusterSpec(request);
      return ((GetClusterSpecResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RegisterWorkerSpecResponseProto registerWorkerSpec(RpcController controller,
                                                            RegisterWorkerSpecRequestProto proto) throws ServiceException {
    RegisterWorkerSpecRequestPBImpl request = new RegisterWorkerSpecRequestPBImpl(proto);
    try {
      RegisterWorkerSpecResponse response = real.registerWorkerSpec(request);
      return ((RegisterWorkerSpecResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RegisterTensorBoardUrlResponseProto registerTensorBoardUrl(
      RpcController controller, RegisterTensorBoardUrlRequestProto proto)
      throws ServiceException {
    RegisterTensorBoardUrlRequestPBImpl request = new RegisterTensorBoardUrlRequestPBImpl(proto);
    try {
      RegisterTensorBoardUrlResponse response = real.registerTensorBoardUrl(request);
      return ((RegisterTensorBoardUrlResponsePBImpl) response).getProto();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RegisterExecutionResultResponseProto registerExecutionResult(
      RpcController controller, RegisterExecutionResultRequestProto proto)
      throws ServiceException {
    RegisterExecutionResultRequestPBImpl request = new RegisterExecutionResultRequestPBImpl(proto);
    try {
      RegisterExecutionResultResponse response = real.registerExecutionResult(request);
      return ((RegisterExecutionResultResponsePBImpl) response).getProto();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EmptyProto finishApplication(RpcController controller, EmptyProto proto)
      throws ServiceException {
    EmptyPBImpl request = new EmptyPBImpl(proto);
    try {
      Empty response = real.finishApplication(request);
      return ((EmptyPBImpl) response).getProto();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public YarnTensorFlowClusterProtos.HeartbeatResponseProto taskExecutorHeartbeat(RpcController controller,
      YarnTensorFlowClusterProtos.HeartbeatRequestProto proto) throws ServiceException {
    HeartbeatRequestPBImpl request = new HeartbeatRequestPBImpl(proto);
    try {
      HeartbeatResponse response = real.taskExecutorHeartbeat(request);
      return ((HeartbeatResponsePBImpl) response).getProto();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }
}
