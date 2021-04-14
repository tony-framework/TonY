/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

public interface RegisterCallbackInfoRequest {
    String getTaskId();
    void setTaskId(String taskId);
    String getCallbackInfo();
    void setCallbackInfo(String callbackInfo);
}
