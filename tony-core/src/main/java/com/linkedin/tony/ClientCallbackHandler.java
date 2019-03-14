/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.rpc.TaskUrl;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Set;

public interface ClientCallbackHandler {
    public void onApplicationIdReceived(ApplicationId appId);
    public void onTaskUrlReceived(Set<TaskUrl> taskUrlSet);
}
