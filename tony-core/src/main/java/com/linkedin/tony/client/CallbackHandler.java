/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.client;

import com.linkedin.tony.rpc.TaskInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Set;

/**
 * CallbackHandler is used to get callbacks from TonyClient when some
 * asynchronous information becomes available.
 */
public interface CallbackHandler {
    // Called when TonyClient gets an application id response from RM.
    public void onApplicationIdReceived(ApplicationId appId);
    // Called when TonyClient gets an application id response from RM.

    // Called when TonyClient gets a set of taskUrls from TonyAM.
    public void onTaskInfosReceived(Set<TaskInfo> taskInfoSet);
}
