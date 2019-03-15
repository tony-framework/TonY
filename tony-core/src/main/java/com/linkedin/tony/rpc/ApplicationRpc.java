/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.yarn.exceptions.YarnException;


public interface ApplicationRpc {
  /**
   * Returns all the task URLs once all tasks have been allocated. Before all tasks have been allocated, this will
   * return an empty set.
   */
  Set<TaskInfo> getTaskInfos() throws IOException, YarnException;

  String getClusterSpec() throws IOException, YarnException;
  String registerWorkerSpec(String worker, String spec) throws IOException, YarnException;
  String registerTensorBoardUrl(String spec) throws Exception;
  String registerExecutionResult(int exitCode, String jobName, String jobIndex, String sessionId) throws Exception;
  void finishApplication() throws YarnException, IOException;
  void taskExecutorHeartbeat(String taskId) throws YarnException, IOException;
  void reset();
}
