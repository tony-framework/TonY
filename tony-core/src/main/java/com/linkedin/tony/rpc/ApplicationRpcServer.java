/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import com.google.protobuf.BlockingService;
import com.linkedin.tony.ServerPortHolder;
import com.linkedin.tony.TonyPolicyProvider;
import com.linkedin.tony.rpc.impl.pb.service.TonyClusterPBServiceImpl;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;


public class ApplicationRpcServer extends Thread implements TonyCluster {
  private static final RecordFactory RECORD_FACTORY = RecordFactoryProvider.getRecordFactory(null);
  private final int rpcPort;
  private final String rpcAddress;
  private final ApplicationRpc appRpc;
  private ClientToAMTokenSecretManager secretManager;
  private Server server;
  private Configuration conf;
  private ServerPortHolder serverPortHolder;

  public ApplicationRpcServer(String hostname, ApplicationRpc rpc, Configuration conf) throws IOException {
    this.rpcAddress = hostname;

    /**
     * Prevent the port from being occupied, port will be kept util rpc server start
     */
    this.rpcPort = ServerPortHolder.getFreePort();
    this.serverPortHolder = new ServerPortHolder(this.rpcPort);
    this.serverPortHolder.start();

    this.appRpc = rpc;
    this.conf = conf;
  }

  @Override
  public GetTaskInfosResponse getTaskInfos(GetTaskInfosRequest request) throws IOException, YarnException {
    GetTaskInfosResponse response = RECORD_FACTORY.newRecordInstance(GetTaskInfosResponse.class);
    response.setTaskInfos(this.appRpc.getTaskInfos());
    return response;
  }

  @Override
  public GetClusterSpecResponse getClusterSpec(GetClusterSpecRequest request)
          throws YarnException, IOException {
    GetClusterSpecResponse response = RECORD_FACTORY.newRecordInstance(GetClusterSpecResponse.class);
    response.setClusterSpec(this.appRpc.getClusterSpec());
    return response;
  }

  @Override
  public RegisterWorkerSpecResponse registerWorkerSpec(RegisterWorkerSpecRequest request)
          throws YarnException, IOException {
    RegisterWorkerSpecResponse response = RECORD_FACTORY.newRecordInstance(RegisterWorkerSpecResponse.class);
    String clusterSpec = this.appRpc.registerWorkerSpec(request.getWorker(), request.getSpec());
    response.setSpec(clusterSpec);
    return response;
  }

  @Override
  public RegisterTensorBoardUrlResponse registerTensorBoardUrl(RegisterTensorBoardUrlRequest request)
          throws Exception {
    RegisterTensorBoardUrlResponse response = RECORD_FACTORY.newRecordInstance(RegisterTensorBoardUrlResponse.class);
    String clusterSpec = this.appRpc.registerTensorBoardUrl(request.getSpec());
    response.setSpec(clusterSpec);
    return response;

  }

  @Override
  public RegisterExecutionResultResponse registerExecutionResult(RegisterExecutionResultRequest request) throws Exception {
    RegisterExecutionResultResponse response = RECORD_FACTORY.newRecordInstance(RegisterExecutionResultResponse.class);
    String msg = this.appRpc.registerExecutionResult(request.getExitCode(), request.getJobName(), request.getJobIndex(), request.getSessionId());
    response.setMessage(msg);
    return response;
  }

  @Override
  public Empty finishApplication(Empty request) throws IOException, YarnException {
    Empty response = RECORD_FACTORY.newRecordInstance(Empty.class);
    this.appRpc.finishApplication();
    return response;
  }

  @Override
  public HeartbeatResponse taskExecutorHeartbeat(HeartbeatRequest request)
      throws YarnException, IOException {
    HeartbeatResponse response = RECORD_FACTORY.newRecordInstance(HeartbeatResponse.class);
    this.appRpc.taskExecutorHeartbeat(request.getTaskId());
    return response;
  }

  @Override
  public Empty registerCallbackInfo(RegisterCallbackInfoRequest request) throws YarnException, IOException {
    Empty response = RECORD_FACTORY.newRecordInstance(Empty.class);
    this.appRpc.registerCallbackInfo(request.getTaskId(), request.getCallbackInfo());
    return response;
  }

  // Reset the Application RPC's state
  public void reset() {
    this.appRpc.reset();
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setSecretManager(ClientToAMTokenSecretManager secretManager) {
    this.secretManager = secretManager;
  }

  @Override
  public void run() {
    try {
      RPC.setProtocolEngine(conf, TonyClusterPB.class, ProtobufRpcEngine.class);
      TonyClusterPBServiceImpl
              translator = new TonyClusterPBServiceImpl(this);
      BlockingService service = com.linkedin.tony.rpc.proto.TonyCluster.TonyClusterService
              .newReflectiveBlockingService(translator);
      serverPortHolder.close();
      server = new RPC.Builder(conf).setProtocol(TonyClusterPB.class)
              .setInstance(service).setBindAddress(rpcAddress)
              .setPort(rpcPort) // TODO: let RPC randomly generate it
              .setSecretManager(secretManager).build();
      server.start();
      if (conf.getBoolean(
              CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
              false)) {
        refreshServiceAcls(conf, new TonyPolicyProvider());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  private synchronized void refreshServiceAcls(Configuration configuration,
                                               PolicyProvider policyProvider) {
    server.refreshServiceAclWithLoadedConfiguration(configuration,
            policyProvider);
  }

  @Override
  public long getProtocolVersion(String protocol, long version) throws IOException {
    return TonyCluster.versionID;
  }


  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
                                                long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this,
            protocol, clientVersion, clientMethodsHash);
  }
}
