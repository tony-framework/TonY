/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterCallbackInfoRequest;
import com.linkedin.tony.rpc.proto.YarnTensorFlowClusterProtos;

public class RegisterCallbackInfoRequestPBImpl implements RegisterCallbackInfoRequest {
    YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto proto = YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto.getDefaultInstance();
    YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto.Builder builder = null;
    private boolean viaProto = false;

    private String taskId = null;
    private String callbackInfo = null;

    public RegisterCallbackInfoRequestPBImpl() {
        builder = YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto.newBuilder();
    }

    public RegisterCallbackInfoRequestPBImpl(YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
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
        if (this.taskId != null) {
            builder.setTaskId(this.taskId);
        }
        if (this.callbackInfo != null) {
            builder.setCallbackInfo(callbackInfo);
        }
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getTaskId() {
        YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.taskId != null) {
            return this.taskId;
        }
        if (!p.hasTaskId()) {
            return null;
        }
        this.taskId = p.getTaskId();
        return this.taskId;
    }

    @Override
    public void setTaskId(String taskId) {
        maybeInitBuilder();
        if (taskId == null) {
            builder.clearTaskId();
        }
        this.taskId = taskId;
    }

    @Override
    public String getCallbackInfo() {
        YarnTensorFlowClusterProtos.RegisterCallbackInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
        if (this.callbackInfo != null) {
            return this.callbackInfo;
        }
        if (!p.hasCallbackInfo()) {
            return callbackInfo;
        }
        this.callbackInfo = p.getCallbackInfo();
        return this.callbackInfo;
    }

    @Override
    public void setCallbackInfo(String callbackInfo) {
        maybeInitBuilder();
        if (callbackInfo == null) {
            builder.clearCallbackInfo();
        }
        this.callbackInfo = callbackInfo;
    }
}
