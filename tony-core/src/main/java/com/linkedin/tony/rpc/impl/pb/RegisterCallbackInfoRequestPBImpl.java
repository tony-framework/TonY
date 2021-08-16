/*
 * Copyright 2021 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.tony.rpc.impl.pb;

import com.linkedin.tony.rpc.RegisterCallbackInfoRequest;
import com.linkedin.tony.rpc.proto.YarnTonyClusterProtos;

public class RegisterCallbackInfoRequestPBImpl implements RegisterCallbackInfoRequest {
    YarnTonyClusterProtos.RegisterCallbackInfoRequestProto proto = YarnTonyClusterProtos.RegisterCallbackInfoRequestProto.getDefaultInstance();
    YarnTonyClusterProtos.RegisterCallbackInfoRequestProto.Builder builder = null;
    private boolean viaProto = false;

    private String taskId = null;
    private String callbackInfo = null;

    public RegisterCallbackInfoRequestPBImpl() {
        builder = YarnTonyClusterProtos.RegisterCallbackInfoRequestProto.newBuilder();
    }

    public RegisterCallbackInfoRequestPBImpl(YarnTonyClusterProtos.RegisterCallbackInfoRequestProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public YarnTonyClusterProtos.RegisterCallbackInfoRequestProto getProto() {
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
            builder = YarnTonyClusterProtos.RegisterCallbackInfoRequestProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public String getTaskId() {
        YarnTonyClusterProtos.RegisterCallbackInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
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
        YarnTonyClusterProtos.RegisterCallbackInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
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
