/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.util.Arrays;
import java.util.List;

public class TonyConfigurationKeys {
  public enum MLFramework {
    TENSORFLOW,
    PYTORCH
  }

  private TonyConfigurationKeys() {

  }

  public static final String TONY_PREFIX = "tony.";

  // Version info configuration
  public static final String TONY_VERSION_INFO_PREFIX = TONY_PREFIX + "version-info.";
  public static final String TONY_VERSION_INFO_VERSION = TONY_VERSION_INFO_PREFIX + "version";
  public static final String TONY_VERSION_INFO_REVISION = TONY_VERSION_INFO_PREFIX + "revision";
  public static final String TONY_VERSION_INFO_BRANCH = TONY_VERSION_INFO_PREFIX + "branch";
  public static final String TONY_VERSION_INFO_USER = TONY_VERSION_INFO_PREFIX + "user";
  public static final String TONY_VERSION_INFO_DATE = TONY_VERSION_INFO_PREFIX + "date";
  public static final String TONY_VERSION_INFO_URL = TONY_VERSION_INFO_PREFIX + "url";
  public static final String TONY_VERSION_INFO_CHECKSUM = TONY_VERSION_INFO_PREFIX + "checksum";

  public static final String OTHER_NAMENODES_TO_ACCESS = TONY_PREFIX + "other.namenodes";

  // History folder configuration
  public static final String TONY_HISTORY_LOCATION = TONY_PREFIX + "history.location";
  public static final String DEFAULT_TONY_HISTORY_LOCATION = "/path/to/tony-history";

  public static final String TONY_HISTORY_INTERMEDIATE = TONY_PREFIX + "history.intermediate";
  public static final String DEFAULT_TONY_HISTORY_INTERMEDIATE = DEFAULT_TONY_HISTORY_LOCATION + "/intermediate";

  public static final String TONY_HISTORY_FINISHED = TONY_PREFIX + "history.finished";
  public static final String DEFAULT_TONY_HISTORY_FINISHED = DEFAULT_TONY_HISTORY_LOCATION + "/finished";

  public static final String TONY_HISTORY_CACHE_MAX_ENTRIES = TONY_PREFIX + "history.cache.max-entries";
  public static final String DEFAULT_TONY_HISTORY_CACHE_MAX_ENTRIES = "1000";

  public static final String TONY_KEYTAB_USER = TONY_PREFIX + "keytab.user";
  public static final String DEFAULT_TONY_KEYTAB_USER = "user";

  public static final String TONY_KEYTAB_LOCATION = TONY_PREFIX + "keytab.location";
  public static final String DEFAULT_TONY_KEYTAB_LOCATION = "/path/to/tony.keytab";

  // All these variables are here to pass TestTonyConfigurationFields.
  // Do not remove unless it is also removed in tony-default.xml.
  public static final String TONY_HTTPS_PORT = TONY_PREFIX + "https.port";
  public static final String DEFAULT_TONY_HTTPS_PORT = "19886";

  public static final String TONY_HTTPS_KEYSTORE_PATH = TONY_PREFIX + "https.keystore.path";
  public static final String DEFAULT_TONY_HTTPS_KEYSTORE_PATH = "/path/to/keystore.jks";

  public static final String TONY_HTTPS_KEYSTORE_TYPE = TONY_PREFIX + "https.keystore.type";
  public static final String DEFAULT_TONY_HTTPS_KEYSTORE_TYPE = "JKS";

  public static final String TONY_HTTPS_KEYSTORE_PASSWORD = TONY_PREFIX + "https.keystore.password";
  public static final String DEFAULT_TONY_HTTPS_KEYSTORE_PASSWORD = "password";

  public static final String TONY_HTTPS_KEYSTORE_ALGORITHM = TONY_PREFIX + "https.keystore.algorithm";
  public static final String DEFAULT_TONY_HTTPS_KEYSTORE_ALGORITHM = "SunX509";

  public static final String TONY_HTTP_PORT = TONY_PREFIX + "http.port";
  public static final String DEFAULT_TONY_HTTP_PORT = "disabled";

  public static final String TONY_SECRET_KEY = TONY_PREFIX + "secret.key";
  public static final String DEFAULT_TONY_SECRET_KEY = "changeme";

  public static final String TONY_HISTORY_MAX_APPEND = TONY_PREFIX + "history.maxAppends";
  public static final int DEFAULT_TONY_HISTORY_MAX_APPEND = 3;

  public static final String TONY_HISTORY_HOST = TONY_PREFIX + "history.host";
  public static final String DEFAULT_TONY_HISTORY_HOST = "https://localhost:" + DEFAULT_TONY_HTTPS_PORT;

  // Application configurations
  public static final String YARN_QUEUE_NAME = TONY_PREFIX + "yarn.queue";
  public static final String DEFAULT_YARN_QUEUE_NAME = "default";

  public static final String TONY_APPLICATION_PREFIX = TONY_PREFIX + "application.";

  public static final String APPLICATION_NAME = TONY_APPLICATION_PREFIX + "name";
  public static final String DEFAULT_APPLICATION_NAME = "TonyApplication";

  public static final String FRAMEWORK_NAME = TONY_APPLICATION_PREFIX + "framework";
  public static final String DEFAULT_FRAMEWORK_NAME = "tensorflow";

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

  /**
   * Max total number of task instances that can be requested across all task types.
   */
  public static final String TONY_MAX_TOTAL_INSTANCES = TONY_TASK_PREFIX + "max-total-instances";

  public static final String TASK_EXECUTOR_JVM_OPTS = TONY_TASK_PREFIX + "executor.jvm.opts";
  public static final String DEFAULT_TASK_EXECUTOR_JVM_OPTS = "-Xmx1536m";

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
  public static final String RESOURCES_REGEX = "tony\\.([a-z]+)\\.resources";
  public static final String DEFAULT_MEMORY = "2g";
  public static final int DEFAULT_VCORES = 1;
  public static final int DEFAULT_GPUS = 0;

  public static String getInstancesKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.instances", jobName);
  }

  /**
   * Configuration key for property controlling how many {@code jobName} task instances a job can request.
   * @param jobName the task type for which to get the max instances config key
   * @return the max instances configuration key for the {@code jobName}
   */
  public static String getMaxInstancesKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.max-instances", jobName);
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

  // Job specific resources
  public static String getResourcesKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.resources", jobName);
  }

  // Resources for all containers
  public static String getContainerResourcesKey() {
    return TONY_PREFIX + "containers.resources";
  }

  // Job specific execution command
  public static String getExecuteCommandKey(String jobName) {
    return String.format(TONY_APPLICATION_PREFIX + "%s.command", jobName);
  }

  // Execution command for all containers
  public static String getContainerExecuteCommandKey() {
    return TONY_APPLICATION_PREFIX + "containers.command";
  }

  // Job specific docker image
  public static String getDockerImageKey(String jobName) {
    return String.format(DOCKER_PREFIX + "%s.image", jobName);
  }

  // Docker images for all containers
  public static String getContainerDockerKey() {
    return DOCKER_PREFIX + "containers.image";
  }

  // Worker configurations
  public static final String WORKER_PREFIX = TONY_PREFIX + "worker.";
  public static final String WORKER_TIMEOUT = WORKER_PREFIX + "timeout";
  public static final int DEFAULT_WORKER_TIMEOUT = 0;

  // Job types that we don't wait to finish
  public static final String UNTRACKED_JOBTYPES = TONY_APPLICATION_PREFIX + "untracked.jobtypes";
  public static final String UNTRACKED_JOBTYPES_DEFAULT = "ps";

  // Training chief configuration
  public static final String CHIEF_PREFIX = TONY_PREFIX + "chief.";

  // Tony with docker configuration
  public static final String DOCKER_PREFIX = TONY_PREFIX + "docker.";
  public static final String DOCKER_ENABLED = DOCKER_PREFIX + "enabled";
  public static final boolean DEFAULT_DOCKER_ENABLED = false;

  // Environment
  public static final String CONTAINER_LAUNCH_ENV = TONY_PREFIX + "containers.envs";
  public static final String EXECUTION_ENV = TONY_PREFIX + "execution.envs";

  // Local testing configurations
  public static final String SECURITY_ENABLED = TONY_APPLICATION_PREFIX + "security.enabled";
  public static final boolean DEFAULT_SECURITY_ENABLED = true;

  public static final String HDFS_CONF_LOCATION = TONY_APPLICATION_PREFIX + "hdfs-conf-path";

  public static final String YARN_CONF_LOCATION = TONY_APPLICATION_PREFIX + "yarn-conf-path";

  // Configurations that can take multiple values.
  public static final List<String> MULTI_VALUE_CONF = Arrays.asList(CONTAINER_LAUNCH_ENV, EXECUTION_ENV,
      getContainerResourcesKey());
}
