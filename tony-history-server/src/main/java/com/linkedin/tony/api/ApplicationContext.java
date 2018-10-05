/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.api;

import com.linkedin.tony.common.InputInfo;
import com.linkedin.tony.common.Message;
import com.linkedin.tony.common.OutputInfo;
import com.linkedin.tony.container.THSContainerId;
import com.linkedin.tony.common.THSContainerStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.mapred.InputSplit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public interface ApplicationContext {

  ApplicationId getApplicationID();

  int getWorkerNum();

  int getPsNum();

  int getWorkerMemory();

  int getPsMemory();

  int getWorkerVCores();

  int getPsVCores();

  List<Container> getWorkerContainers();

  List<Container> getPsContainers();

  THSContainerStatus getContainerStatus(THSContainerId containerId);

  List<InputInfo> getInputs(THSContainerId containerId);

  List<InputSplit> getStreamInputs(THSContainerId containerId);

  List<OutputInfo> getOutputs();

  LinkedBlockingQueue<Message> getMessageQueue();

  String getTensorBoardUrl();

  Map<THSContainerId, String> getReporterProgress();

  Map<THSContainerId, String> getContainersAppStartTime();

  Map<THSContainerId, String> getContainersAppFinishTime();

  Map<THSContainerId, String> getMapedTaskID();

  Map<THSContainerId, ConcurrentHashMap<String, LinkedBlockingDeque<Object>>> getContainersCpuMetrics();

  Map<THSContainerId, ConcurrentHashMap<String, List<Double>>> getContainersCpuStatistics();

  int getSavingModelStatus();

  int getSavingModelTotalNum();

  Boolean getStartSavingStatus();

  void startSavingModelStatus(Boolean flag);

  Boolean getLastSavingStatus();

  List<Long> getModelSavingList();

}
