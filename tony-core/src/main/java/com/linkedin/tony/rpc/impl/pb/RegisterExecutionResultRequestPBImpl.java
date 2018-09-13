/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterExecutionResultRequest;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos.RegisterExecutionResultRequestProto;


public class RegisterExecutionResultRequestPBImpl implements RegisterExecutionResultRequest {
  private RegisterExecutionResultRequestProto proto = RegisterExecutionResultRequestProto.getDefaultInstance();
  private RegisterExecutionResultRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private String jobName = null;
  private String jobIndex = null;
  private String sessionId = null;

  public RegisterExecutionResultRequestPBImpl() {
    builder = RegisterExecutionResultRequestProto.newBuilder();
  }

  public RegisterExecutionResultRequestPBImpl(RegisterExecutionResultRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.jobName != null) {
      builder.setJobName(this.jobName);
    }
    if (this.jobIndex != null) {
      builder.setJobIndex(this.jobIndex);
    }
    if (this.sessionId != null) {
      builder.setSessionId(this.sessionId);
    }
  }

  public RegisterExecutionResultRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterExecutionResultRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getExitCode() {
    YarnTensorFlowClusterProtos.RegisterExecutionResultRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExitCode();
  }

  @Override
  public void setExitCode(int exitCode) {
    maybeInitBuilder();
    builder.setExitCode(exitCode);
  }


  @Override
  public String getJobName() {
    YarnTensorFlowClusterProtos.RegisterExecutionResultRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobName != null) {
      return this.jobName;
    }
    if (!p.hasJobName()) {
      return null;
    }
    this.jobName = p.getJobName();
    return this.jobName;
  }

  @Override
  public void setJobName(String jobName) {
    maybeInitBuilder();
    if (jobName == null) {
      builder.clearJobName();
    }
    this.jobName = jobName;
  }

  @Override
  public String getJobIndex() {
    YarnTensorFlowClusterProtos.RegisterExecutionResultRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobIndex != null) {
      return this.jobIndex;
    }
    if (!p.hasJobIndex()) {
      return null;
    }
    this.jobIndex = p.getJobIndex();
    return this.jobIndex;
  }

  @Override
  public void setJobIndex(String jobIndex) {
    maybeInitBuilder();
    if (jobIndex == null) {
      builder.clearJobIndex();
    }
    this.jobIndex = jobIndex;
  }

  @Override
  public String getSessionId() {
    YarnTensorFlowClusterProtos.RegisterExecutionResultRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.sessionId != null) {
      return this.sessionId;
    }
    if (!p.hasSessionId()) {
      return null;
    }
    this.sessionId = p.getSessionId();
    return this.sessionId;
  }

  @Override
  public void setSessionId(String sessionId) {
    maybeInitBuilder();
    if (sessionId == null) {
      builder.clearSessionId();
    }
    this.sessionId = sessionId;
  }
}
