/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.api;

import com.linkedin.tony.common.HeartbeatRequest;
import com.linkedin.tony.common.HeartbeatResponse;
import com.linkedin.tony.common.InputInfo;
import com.linkedin.tony.common.OutputInfo;
import com.linkedin.tony.container.THSContainerId;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.InputSplit;

public interface ApplicationContainerProtocol extends VersionedProtocol {

  public static final long versionID = 1L;

  void reportReservedPort(String host, int port, String role, int index);

  void reportLightGbmIpPort(THSContainerId containerId, String lightGbmIpPort);

  String getLightGbmIpPortStr();

  void reportLightLDAIpPort(THSContainerId containerId, String lightLDAIpPort);

  String getLightLDAIpPortStr();

  String getClusterDef();

  HeartbeatResponse heartbeat(THSContainerId containerId, HeartbeatRequest heartbeatRequest);

  InputInfo[] getInputSplit(THSContainerId containerId);

  InputSplit[] getStreamInputSplit(THSContainerId containerId);

  OutputInfo[] getOutputLocation();

  void reportTensorBoardURL(String url);

  void reportMapedTaskID(THSContainerId containerId, String taskId);

  void reportCpuMetrics(THSContainerId containerId, String cpuMetrics);

  Long interResultTimeStamp();

}
