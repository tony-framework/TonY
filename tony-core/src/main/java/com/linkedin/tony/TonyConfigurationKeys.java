/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

public class TonyConfigurationKeys {

  private TonyConfigurationKeys() {

  }

  public static final String TONY_PREFIX = "tony.";

  public static final String OTHER_NAMENODES_TO_ACCESS = TONY_PREFIX + "other.namenodes";

  // Application configurations
  public static final String YARN_QUEUE_NAME = TONY_PREFIX + "yarn.queue";
  public static final String DEFAULT_YARN_QUEUE_NAME = "default";

  public static final String TONY_APPLICATION_PREFIX = TONY_PREFIX + "application.";

  public static final String APPLICATION_NAME = TONY_APPLICATION_PREFIX + "name";
  public static final String DEFAULT_APPLICATION_NAME = "TensorFlowApplication";

  public static final String APPLICATION_NODE_LABEL = TONY_APPLICATION_PREFIX + "node-label";

  public static final String IS_SINGLE_NODE = TONY_APPLICATION_PREFIX + "single-node";
  public static final boolean DEFAULT_IS_SINGLE_NODE = false;

  public static final String ENABLE_PREPROCESSING_JOB = TONY_APPLICATION_PREFIX + "enable-preprocess";
  public static final boolean DEFAULT_ENABLE_PREPROCESSING_JOB = false;

  public static final String APPLICATION_TIMEOUT = TONY_APPLICATION_PREFIX + "timeout";
  public static final int DEFAULT_APPLICATION_TIMEOUT = 0;

  public static final String RM_CONNECT_RETRY_MULTIPLIER = TONY_APPLICATION_PREFIX + "num-rm-connect-retries";
  public static final int DEFAULT_RM_CONNECT_RETRY_MULTIPLIER = 1;

  // Task configurations
  public static final String TONY_TASK_PREFIX = TONY_PREFIX + "task.";

  public static final String TASK_EXECUTOR_JVM_OPTS = TONY_TASK_PREFIX + "executor.jvm.opts";
  public static final String DEFAULT_TASK_EXECUTOR_JVM_OPTS = "-Xmx1536m";

  public static final String TASK_REGISTRATION_TIMEOUT_SEC = TONY_TASK_PREFIX + "registration-timeout-sec";
  public static final int DEFAULT_TASK_REGISTRATION_TIMEOUT_SEC = 300;

  public static final String TASK_REGISTRATION_RETRY_COUNT = TONY_TASK_PREFIX + "registration-retry-count";
  public static final int DEFAULT_TASK_REGISTRATION_RETRY_COUNT = 0;

  public static final String TASK_HEARTBEAT_INTERVAL_MS = TONY_TASK_PREFIX + "heartbeat-interval";
  public static final int DEFAULT_TASK_HEARTBEAT_INTERVAL_MS = 1000;

  public static final String TASK_MAX_MISSED_HEARTBEATS = TONY_TASK_PREFIX + "max-missed-heartbeats";
  public static final int DEFAULT_TASK_MAX_MISSED_HEARTBEATS = 25;

  // AM configurations
  public static final String AM_PREFIX = TONY_PREFIX + "am.";

  public static final String AM_RETRY_COUNT = AM_PREFIX + "retry-count";
  public static final int DEFAULT_AM_RETRY_COUNT = 0;

  public static final String AM_MEMORY = AM_PREFIX + "memory";
  public static final String DEFAULT_AM_MEMORY = "2g";

  public static final String AM_VCORES = AM_PREFIX + "vcores";
  public static final int DEFAULT_AM_VCORES = 1;

  public static final String AM_GPUS = AM_PREFIX + "gpus";
  public static final int DEFAULT_AM_GPUS = 0;

  // PS configurations
  public static final String PS_PREFIX = TONY_PREFIX + "ps.";

  public static final String PS_MEMORY = PS_PREFIX + "memory";
  public static final String DEFAULT_PS_MEMORY = "2g";

  public static final String PS_VCORES = PS_PREFIX + "vcores";
  public static final int DEFAULT_PS_VCORES = 1;

  public static final String PS_INSTANCES = PS_PREFIX + "instances";
  public static final int DEFAULT_PS_INSTANCES = 1;

  // Worker configurations
  public static final String WORKER_PREFIX = TONY_PREFIX + "worker.";

  public static final String WORKER_TIMEOUT = WORKER_PREFIX + "timeout";
  public static final int DEFAULT_WORKER_TIMEOUT = 0;

  public static final String WORKER_MEMORY = WORKER_PREFIX + "memory";
  public static final String DEFAULT_WORKER_MEMORY = "2g";

  public static final String WORKER_VCORES = WORKER_PREFIX  + "vcores";
  public static final int DEFAULT_WORKER_VCORES = 1;

  public static final String WORKER_GPUS = WORKER_PREFIX + "gpus";
  public static final int DEFAULT_WORKER_GPUS = 0;

  public static final String WORKER_INSTANCES = WORKER_PREFIX + "instances";
  public static final int DEFAULT_WORKER_INSTANCES = 1;

  // Local testing configurations
  public static final String IS_INSECURE_MODE = TONY_APPLICATION_PREFIX + "insecure-mode";
  public static final boolean DEFAULT_IS_INSECURE_MODE = false;

  public static final String HDFS_CONF_LOCATION = TONY_APPLICATION_PREFIX + "hdfs-conf-path";

  public static final String YARN_CONF_LOCATION = TONY_APPLICATION_PREFIX + "yarn-conf-path";

}
