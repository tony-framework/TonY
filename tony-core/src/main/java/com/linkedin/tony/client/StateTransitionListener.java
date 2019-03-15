/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.client;

import com.linkedin.tony.rpc.TaskInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.Set;

public interface StateTransitionListener {
    // Called when TonyClient gets a set of taskUrls from TonyAM.
    public void onTaskInfosReceived(Set<TaskInfo> taskInfoSet);
}
