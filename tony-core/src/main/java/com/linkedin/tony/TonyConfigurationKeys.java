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

  public static final String RM_CLIENT_CONNECT_RETRY_MULTIPLIER = TONY_APPLICATION_PREFIX + "num-client-rm-connect-retries";
  public static final int DEFAULT_RM_CLIENT_CONNECT_RETRY_MULTIPLIER = 3;

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

  // Keys/default values for configurable TensorFlow job names
  public static final String INSTANCES_REGEX = "tony\\.([a-z]+)\\.instances";
  public static final String DEFAULT_MEMORY = "2g";
  public static final int DEFAULT_VCORES = 1;
  public static final int DEFAULT_GPUS = 0;

  public static String getInstancesKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.instances", jobName);
  }

  public static int getDefaultInstances(String jobName) {
    switch (jobName) {
      case Constants.PS_JOB_NAME:
      case Constants.WORKER_JOB_NAME:
        return 1;
      default:
        return 0;
    }
  }
  public static String getMemoryKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.memory", jobName);
  }

  public static String getVCoresKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.vcores", jobName);
  }

  public static String getGPUsKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.gpus", jobName);
  }

  // Worker configurations
  public static final String WORKER_PREFIX = TONY_PREFIX + "worker.";

  public static final String WORKER_TIMEOUT = WORKER_PREFIX + "timeout";
  public static final int DEFAULT_WORKER_TIMEOUT = 0;

  // Local testing configurations
  public static final String SECURITY_ENABLED = TONY_APPLICATION_PREFIX + "security.enabled";
  public static final boolean DEFAULT_SECURITY_ENABLED = true;

  public static final String HDFS_CONF_LOCATION = TONY_APPLICATION_PREFIX + "hdfs-conf-path";

  public static final String YARN_CONF_LOCATION = TONY_APPLICATION_PREFIX + "yarn-conf-path";

  // Docker related
  public static final String IS_DOCKER_MODE = TONY_APPLICATION_PREFIX + "docker-mode";
  public static final boolean DEFAULT_IS_DOCKER_MODE = false;

  public static final String DOCKER_BINARY_PATH = TONY_APPLICATION_PREFIX + "docker-path";
  public static final String DEFAULT_DOCKER_BINARY_PATH = "docker";

  public static final String DOCKER_IMAGE_PATH = TONY_APPLICATION_PREFIX + "docker-image"; // Required for Docker mode.
}
