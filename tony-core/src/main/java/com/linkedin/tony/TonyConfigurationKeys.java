/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.linkedin.tony.Constants.SIDECAR_TB_ROLE_NAME;

public class TonyConfigurationKeys {
  public enum FrameworkType {
    TENSORFLOW,
    PYTORCH,
    HOROVOD,
    MXNET,
    STANDALONE
  }

  public enum DistributedMode {
    GANG,
    FCFS
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

  // History-related configuration
  public static final String TONY_HISTORY_PREFIX = TONY_PREFIX + "history.";

  public static final String TONY_HISTORY_LOCATION = TONY_HISTORY_PREFIX + "location";
  public static final String DEFAULT_TONY_HISTORY_LOCATION = "/path/to/tony-history";

  public static final String TONY_HISTORY_INTERMEDIATE = TONY_HISTORY_PREFIX + "intermediate";
  public static final String DEFAULT_TONY_HISTORY_INTERMEDIATE = DEFAULT_TONY_HISTORY_LOCATION + "/intermediate";

  public static final String TONY_HISTORY_FINISHED = TONY_HISTORY_PREFIX + "finished";
  public static final String DEFAULT_TONY_HISTORY_FINISHED = DEFAULT_TONY_HISTORY_LOCATION + "/finished";

  public static final String TONY_HISTORY_MOVER_INTERVAL_MS = TONY_HISTORY_PREFIX + "mover-interval-ms";
  public static final int DEFAULT_TONY_HISTORY_MOVER_INTERVAL_MS = 5 * 60 * 1000;

  public static final String TONY_HISTORY_FINISHED_DIR_TIMEZONE = TONY_HISTORY_PREFIX + "finished-dir-timezone";
  public static final String DEFAULT_TONY_HISTORY_FINISHED_DIR_TIMEZONE = "UTC";

  // How many seconds to retain history files for
  public static final String TONY_HISTORY_RETENTION_SECONDS = TONY_HISTORY_PREFIX + "retention-sec";
  public static final int DEFAULT_TONY_HISTORY_RETENTION_SECONDS = 30 * 24 * 60 * 60;

  // How frequently to run the purger thread
  public static final String TONY_HISTORY_PURGER_INTERVAL_MS = TONY_HISTORY_PREFIX + "purger-interval-ms";
  public static final int DEFAULT_TONY_HISTORY_PURGER_INTERVAL_MS = 6 * 60 * 60 * 1000;

  public static final String TONY_PORTAL_CACHE_MAX_ENTRIES = TONY_PREFIX + "portal.cache.max-entries";
  public static final String DEFAULT_TONY_PORTAL_CACHE_MAX_ENTRIES = "10000";

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

  public static final String TONY_PORTAL_URL = TONY_PREFIX + "portal.url";
  public static final String DEFAULT_TONY_PORTAL_URL = "https://localhost:" + DEFAULT_TONY_HTTPS_PORT;

  // Application configurations
  public static final String YARN_QUEUE_NAME = TONY_PREFIX + "yarn.queue";
  public static final String DEFAULT_YARN_QUEUE_NAME = "default";

  public static final String TONY_APPLICATION_PREFIX = TONY_PREFIX + "application.";

  public static final String APPLICATION_NAME = TONY_APPLICATION_PREFIX + "name";
  public static final String DEFAULT_APPLICATION_NAME = "TonyApplication";

  public static final String APPLICATION_TYPE = TONY_APPLICATION_PREFIX + "type";
  public static final String DEFAULT_APPLICATION_TYPE = Constants.APP_TYPE;

  public static final String FRAMEWORK_NAME = TONY_APPLICATION_PREFIX + "framework";
  public static final String DEFAULT_FRAMEWORK_NAME = "tensorflow";

  public static final String APPLICATION_NODE_LABEL = TONY_APPLICATION_PREFIX + "node-label";

  public static final String ENABLE_PREPROCESSING_JOB = TONY_APPLICATION_PREFIX + "enable-preprocess";
  public static final boolean DEFAULT_ENABLE_PREPROCESSING_JOB = false;

  public static final String APPLICATION_TIMEOUT = TONY_APPLICATION_PREFIX + "timeout";
  public static final int DEFAULT_APPLICATION_TIMEOUT = 0;

  public static final String RM_CLIENT_CONNECT_RETRY_MULTIPLIER = TONY_APPLICATION_PREFIX + "num-client-rm-connect-retries";
  public static final int DEFAULT_RM_CLIENT_CONNECT_RETRY_MULTIPLIER = 3;

  public static final String APPLICATION_TAGS = TONY_APPLICATION_PREFIX + "tags";

  public static final String APPLICATION_PREPARE_STAGE = TONY_APPLICATION_PREFIX + "prepare-stage";
  public static final String APPLICATION_TRAINING_STAGE = TONY_APPLICATION_PREFIX + "training-stage";

  public static final String APPLICATION_DISTRIBUTED_MODE = TONY_APPLICATION_PREFIX + "distributed-mode";
  public static final String DEFAULT_APPLICATION_DISTRIBUTED_MODE = DistributedMode.GANG.name();

  public static final String TONY_HADOOP_PREFIX = TONY_APPLICATION_PREFIX + "hadoop.";
  public static final String APPLICATION_HADOOP_LOCATION = TONY_HADOOP_PREFIX + "location";
  public static final String APPLICATION_HADOOP_CLASSPATH = TONY_HADOOP_PREFIX + "classpath";

  // Task configurations
  public static final String TONY_TASK_PREFIX = TONY_PREFIX + "task.";

  /**
   * Max total number of task instances that can be requested across all task types.
   */
  public static final String MAX_TOTAL_INSTANCES = TONY_TASK_PREFIX + "max-total-instances";
  public static final int DEFAULT_MAX_TOTAL_INSTANCES = -1;

  public static final String TASK_AM_JVM_OPTS = TONY_TASK_PREFIX + "am.jvm.opts";

  public static final String TASK_EXECUTOR_JVM_OPTS = TONY_TASK_PREFIX + "executor.jvm.opts";
  public static final String DEFAULT_TASK_EXECUTOR_JVM_OPTS = "-Xmx1536m";

  public static final String TASK_HEARTBEAT_INTERVAL_MS = TONY_TASK_PREFIX + "heartbeat-interval-ms";
  public static final int DEFAULT_TASK_HEARTBEAT_INTERVAL_MS = 1000;

  public static final String TASK_MAX_MISSED_HEARTBEATS = TONY_TASK_PREFIX + "max-missed-heartbeats";
  public static final int DEFAULT_TASK_MAX_MISSED_HEARTBEATS = 25;

  public static final String TASK_METRICS_UPDATE_INTERVAL_MS = TONY_TASK_PREFIX + "metrics-interval-ms";
  public static final int DEFAULT_TASK_METRICS_UPDATE_INTERVAL_MS = 5000;

  public static final String TASK_GPU_METRICS_ENABLED = TONY_TASK_PREFIX + "gpu-metrics.enabled";
  public static final boolean DEFAULT_TASK_GPU_METRICS_ENABLED = true;

  public static final String TASK_EXECUTION_TIMEOUT = TONY_TASK_PREFIX + "executor.execution-timeout-ms";
  public static final int DEFAULT_TASK_EXECUTION_TIMEOUT = 0;

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

  public static final String AM_WAIT_CLIENT_STOP_TIMEOUT = AM_PREFIX + "wait-client-signal-stop-timeout-sec";
  public static final int DEFAULT_AM_WAIT_CLIENT_STOP_TIMEOUT = 15;

  // Keys/default values for configurable TensorFlow job names
  public static final String INSTANCES_REGEX = "tony\\.([a-z]+)\\.instances";
  public static final String MAX_TOTAL_RESOURCES_REGEX = TONY_TASK_PREFIX + "max-total-([a-z]+)";
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

  public static String getResourceKey(String jobName, String resource) {
    return String.format(TONY_PREFIX + "%s.%s", jobName, resource);
  }

  public static String getNodeLabelKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.node-label", jobName);
  }

  public static String getDependsOnKey(String jobName) {
    return String.format(TONY_PREFIX + "%s.depends-on", jobName);
  }

  public static String getMaxTotalResourceKey(String resource) {
    return String.format(TONY_TASK_PREFIX + "max-total-%s", resource);
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
    return String.format(TONY_PREFIX + "%s.command", jobName);
  }

  // Execution command for all containers
  public static String getContainerExecuteCommandKey() {
    return TONY_PREFIX + "containers.command";
  }

  // Job specific docker image
  public static String getDockerImageKey(String jobName) {
    return String.format(DOCKER_PREFIX + "%s.image", jobName);
  }

  // Docker images for all containers
  public static String getContainerDockerKey() {
    return DOCKER_PREFIX + "containers.image";
  }

  public static String getContainerDockerMountKey() {
    return DOCKER_PREFIX + "containers.mount";
  }

  // Container configurations
  public static final String CONTAINER_PREFIX = TONY_PREFIX + "container.";
  public static final String CONTAINER_REGISTRATION_TIMEOUT = CONTAINER_PREFIX + "registration.timeout";
  public static final int DEFAULT_CONTAINER_REGISTRATION_TIMEOUT = 15 * 60 * 1000;
  public static final String CONTAINER_ALLOCATION_TIMEOUT = CONTAINER_PREFIX + "allocation.timeout";
  public static final int DEFAULT_CONTAINER_ALLOCATION_TIMEOUT = 0;

  // Job types that we don't wait to finish
  public static final String UNTRACKED_JOBTYPES = TONY_APPLICATION_PREFIX + "untracked.jobtypes";
  public static final String UNTRACKED_JOBTYPES_DEFAULT = "ps";

  // Specified untracked job type timeout when all tracked tasks finished
  public static final String UNTRACKED_JOBTYPE_TIMEOUT_REGEX = TONY_APPLICATION_PREFIX + "([a-z]+)\\.untracked.timeout";
  public static final long UNTRACKED_JOBTYPE_TIMEOUT_DEFAULT = 0;

  // Job types that we don't wait to finish and ignore its failure.
  public static final String SIDECAR_JOBTYPES = TONY_APPLICATION_PREFIX + "sidecar.jobtypes";
  public static final String DEFAULT_SIDECAR_JOBTYPES = SIDECAR_TB_ROLE_NAME;

  // Job types that we will short circuit when it failed
  public static final String STOP_ON_FAILURE_JOBTYPES = TONY_APPLICATION_PREFIX + "stop-on-failure-jobtypes";

  // Tony configuration to return failure when a worker failed
  public static final String FAIL_ON_WORKER_FAILURE_ENABLED = TONY_APPLICATION_PREFIX + "fail-on-worker-failure-enabled";
  public static final boolean DEFAULT_FAIL_ON_WORKER_FAILURE_ENABLED = false;

  // Tony with docker configuration
  public static final String DOCKER_PREFIX = TONY_PREFIX + "docker.";
  public static final String DOCKER_ENABLED = DOCKER_PREFIX + "enabled";
  public static final boolean DEFAULT_DOCKER_ENABLED = false;

  // Environment
  public static final String CONTAINER_LAUNCH_ENV = TONY_PREFIX + "containers.envs";
  public static final String EXECUTION_ENV = TONY_PREFIX + "execution.envs";
  public static final String GPU_PATH_TO_EXEC = TONY_PREFIX + "gpu-exec-path";
  public static final String DEFAULT_GPU_PATH_TO_EXEC = "nvidia-smi";
  public static final String PYTHON_EXEC_PATH = TONY_PREFIX + "python-exec-path";

  // Local testing configurations
  public static final String SECURITY_ENABLED = TONY_APPLICATION_PREFIX + "security.enabled";
  public static final boolean DEFAULT_SECURITY_ENABLED = true;

  public static final String HDFS_CONF_LOCATION = TONY_APPLICATION_PREFIX + "hdfs-conf-path";

  public static final String YARN_CONF_LOCATION = TONY_APPLICATION_PREFIX + "yarn-conf-path";

  public static final String MAPRED_CONF_LOCATION = TONY_APPLICATION_PREFIX + "mapred-conf-path";

  // Configurations that can take multiple values.
  public static final List<String> MULTI_VALUE_CONF = Collections.unmodifiableList(
      Arrays.asList(CONTAINER_LAUNCH_ENV, EXECUTION_ENV, getContainerResourcesKey()));

  // For Horovod
  public static final String TONY_HOROVOD_PREFIX = TONY_PREFIX + "horovod.";
  // Local testing horovod driver
  public static final String TEST_HOROVOD_FAIL_ENABLE_KEY = TONY_HOROVOD_PREFIX + "mode.test.fast.fail";
  public static final boolean DEFAULT_TEST_HOROVOD_FAIL = false;
  public static final String IN_TEST_HOROVOD_MODE = TONY_HOROVOD_PREFIX + "mode.test";
  public static final boolean DEFAULT_IN_TEST_HOROVOD_MODE = false;

  public static final String HOROVOD_DRIVER_DEBUG_MODE_ENABLE = TONY_HOROVOD_PREFIX + "driver.mode.debug";
  public static final boolean DEFAULT_HOROVOD_DEBUG_MODE_ENABLE = false;

  // Set Tensorboard log dir to start it
  public static final String TENSORBOARD_LOG_DIR = TONY_APPLICATION_PREFIX + "tensorboard-log-dir";

  public static final String TB_JOB_PREFIX = TONY_PREFIX + SIDECAR_TB_ROLE_NAME + ".";

  public static final String TB_VCORE = TB_JOB_PREFIX + "vcores";
  public static final int DEFAULT_TB_VCORE = 2;

  public static final String TB_INSTANCES = TB_JOB_PREFIX + "instances";
  public static final int DEFAULT_TB_INSTANCES = 1;

  public static final String TB_MEMORY = TB_JOB_PREFIX + "memory";
  public static final String DEFAULT_TB_MEMORY = "2g";

  public static final String TB_GPUS = TB_JOB_PREFIX + "gpus";
  public static final int DEFAULT_TB_GPUS = 0;
}
