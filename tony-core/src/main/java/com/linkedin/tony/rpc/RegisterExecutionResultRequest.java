/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.rpc;


public interface RegisterExecutionResultRequest {

  int getExitCode();
  void setExitCode(int exitCode);
  String getJobName();
  void setJobName(String jobName);
  String getJobIndex();
  void setJobIndex(String jobIndex);
  String getSessionId();
  void setSessionId(String sessionId);
}
