/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl;


import com.linkedin.tony.rpc.Empty;
import com.linkedin.tony.rpc.GetClusterSpecRequest;
import com.linkedin.tony.rpc.GetClusterSpecResponse;
import com.linkedin.tony.rpc.GetTaskInfosRequest;
import com.linkedin.tony.rpc.GetTaskInfosResponse;
import com.linkedin.tony.rpc.HeartbeatRequest;
import com.linkedin.tony.rpc.RegisterExecutionResultRequest;
import com.linkedin.tony.rpc.RegisterExecutionResultResponse;
import com.linkedin.tony.rpc.RegisterTensorBoardUrlRequest;
import com.linkedin.tony.rpc.RegisterTensorBoardUrlResponse;
import com.linkedin.tony.rpc.RegisterWorkerSpecRequest;
import com.linkedin.tony.rpc.RegisterWorkerSpecResponse;
import com.linkedin.tony.rpc.ApplicationRpc;
import com.linkedin.tony.rpc.TensorFlowCluster;
import com.linkedin.tony.rpc.TaskInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;


public class ApplicationRpcClient implements ApplicationRpc {
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private TensorFlowCluster tensorflow;
  private static ApplicationRpcClient instance = null;
  private static int port = 0;
  private static String address = "";

  public static ApplicationRpcClient getInstance(String serverAddress, int serverPort, Configuration conf) {
    if (null == instance || !serverAddress.equals(address) || serverPort != port) {
      instance = new ApplicationRpcClient(serverAddress, serverPort, conf);
      address = serverAddress;
      port = serverPort;
    }
    return instance;
  }

  private ApplicationRpcClient(String serverAddress, int serverPort, Configuration conf) {
    InetSocketAddress address = new InetSocketAddress(serverAddress, serverPort);
    YarnRPC rpc;
    rpc = YarnRPC.create(conf);

    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            10, 2000, TimeUnit.MILLISECONDS);
    this.tensorflow = getProxy(conf, rpc, ugi, address, TensorFlowCluster.class, retryPolicy);
  }

  private static <T> T getProxy(final Configuration conf, final YarnRPC rpc, final UserGroupInformation user,
      final InetSocketAddress serverAddress, final Class<T> protocol, RetryPolicy retryPolicy) {
    T proxy = user.doAs((PrivilegedAction<T>) () -> (T) rpc.getProxy(protocol, serverAddress, conf));
    return (T) RetryProxy.create(protocol, proxy, retryPolicy);
  }

  @Override
  public Set<TaskInfo> getTaskInfos() throws IOException, YarnException {
    GetTaskInfosResponse response =
        tensorflow.getTaskInfos(recordFactory.newRecordInstance(GetTaskInfosRequest.class));
    return response.getTaskInfos();
  }

  @Override
  public String getClusterSpec() throws IOException, YarnException {
    GetClusterSpecResponse response =
        tensorflow.getClusterSpec(recordFactory.newRecordInstance(GetClusterSpecRequest.class));
    return response.getClusterSpec();
  }

  @Override
  public String registerWorkerSpec(String worker, String spec) throws IOException, YarnException {
    RegisterWorkerSpecRequest request = recordFactory.newRecordInstance(RegisterWorkerSpecRequest.class);
    request.setWorker(worker);
    request.setSpec(spec);
    RegisterWorkerSpecResponse response = tensorflow.registerWorkerSpec(request);
    return response.getSpec();
  }

  @Override
  public String registerTensorBoardUrl(String spec) throws Exception {
    RegisterTensorBoardUrlRequest request = recordFactory.newRecordInstance(RegisterTensorBoardUrlRequest.class);
    request.setSpec(spec);
    RegisterTensorBoardUrlResponse response = tensorflow.registerTensorBoardUrl(request);
    return response.getSpec();
  }

  @Override
  public String registerExecutionResult(int exitCode, String jobName, String jobIndex, String sessionId) throws Exception {
    RegisterExecutionResultRequest request = recordFactory.newRecordInstance(RegisterExecutionResultRequest.class);
    request.setExitCode(exitCode);
    request.setJobName(jobName);
    request.setJobIndex(jobIndex);
    request.setSessionId(sessionId);
    RegisterExecutionResultResponse response = tensorflow.registerExecutionResult(request);
    return response.getMessage();
  }

  @Override
  public void finishApplication() throws YarnException, IOException {
    Empty request = recordFactory.newRecordInstance(Empty.class);
    tensorflow.finishApplication(request);
  }

  @Override
  public void taskExecutorHeartbeat(String taskId) throws YarnException, IOException {
    HeartbeatRequest request = recordFactory.newRecordInstance(HeartbeatRequest.class);
    request.setTaskId(taskId);
    tensorflow.taskExecutorHeartbeat(request);
  }

  public void reset() { }
}
