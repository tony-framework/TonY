/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class Constants {
  // For capatibility with older Hadoop versions
  public static final String GPU_URI = "yarn.io/gpu";
  public static final String SET_RESOURCE_VALUE_METHOD = "setResourceValue";
  public static final String CONTAINER_RUNTIME_CONSTANTS_CLASS =
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants";
  public static final String DOCKER_LINUX_CONTAINER_RUNTIME_CLASS =
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime";
  public static final String ENV_CONTAINER_TYPE = "ENV_CONTAINER_TYPE";
  public static final String ENV_DOCKER_CONTAINER_IMAGE = "ENV_DOCKER_CONTAINER_IMAGE";
  public static final String SET_MONITOR_INTERVAL_METHOD = "setMonitorInterval";

  // File Permission
  public static final FsPermission PERM770 = new FsPermission((short) 0770);
  public static final FsPermission PERM777 = new FsPermission((short) 0777);

  // History Server related constants
  public static final String JOBS_SUFFIX = "jobs";
  public static final String CONFIG_SUFFIX = "config";
  public static final String HISTFILE_SUFFIX = "jhist";
  public static final String INPROGRESS = "inprogress";
  public static final String SUCCEEDED = "SUCCEEDED";
  public static final String FAILED = "FAILED";
  public static final String RUNNING = "RUNNING";
  public static final String TIME_FORMAT = "dd MMM yyyy HH:mm:ss:SSS Z";

  // TensorFlow constants
  public static final String TB_PORT = "TB_PORT";
  public static final String TASK_INDEX = "TASK_INDEX";
  public static final String TASK_NUM = "TASK_NUM";
  public static final String IS_CHIEF = "IS_CHIEF";
  public static final String CLUSTER_SPEC = "CLUSTER_SPEC";
  public static final String TF_CONFIG = "TF_CONFIG";

  // PyTorch constants
  public static final String COORDINATOR_ID = "worker:0";
  public static final String COMMUNICATION_BACKEND = "tcp://";
  public static final String RANK = "RANK";
  public static final String WORLD = "WORLD";
  public static final String INIT_METHOD = "INIT_METHOD";

  // Distributed TensorFlow job name, e.g. "ps" or "worker",
  // as per https://www.tensorflow.org/deploy/distributed
  public static final String JOB_NAME = "JOB_NAME";
  public static final String SESSION_ID = "SESSION_ID";
  public static final String PREPROCESSING_JOB = "PREPROCESSING_JOB";

  // Environment variables for resource localization
  public static final String TONY_CONF_PREFIX = "TONY_CONF";
  public static final String ARCHIVE_SUFFIX = "#archive";
  public static final String RESOURCE_DIVIDER = "::";

  public static final String PATH_SUFFIX = "_PATH";
  public static final String TIMESTAMP_SUFFIX = "_TIMESTAMP";
  public static final String LENGTH_SUFFIX = "_LENGTH";

  public static final String TONY_SRC_ZIP_NAME = "tony_src.zip";
  public static final String TONY_JAR_NAME = "tony.jar";

  public static final String PYTHON_VENV_ZIP = "venv.zip";
  public static final String PYTHON_VENV_DIR = "venv";
  public static final String TASK_PARAM_KEY = "MODEL_PARAMS";

  public static final String AM_HOST = "AM_HOST";
  public static final String AM_PORT = "AM_PORT";
  public static final String AM_STDOUT_FILENAME = "amstdout.log";
  public static final String AM_STDERR_FILENAME = "amstderr.log";

  public static final String HDFS_SITE_CONF = "hdfs-site.xml";
  public static final String HDFS_DEFAULT_CONF = "hdfs-default.xml";
  public static final String YARN_SITE_CONF = YarnConfiguration.YARN_SITE_CONFIGURATION_FILE;
  public static final String YARN_DEFAULT_CONF = "yarn-default.xml";
  public static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
  public static final String CORE_DEFAULT_CONF = "core-default.xml";
  public static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();
  public static final String TONY_SITE_CONF = "tony-site.xml";
  public static final String TONY_CONF_DIR = "TONY_CONF_DIR";
  // TODO: Remove this default once TONY_CONF_DIR env var is set globally on client machines
  public static final String DEFAULT_TONY_CONF_DIR = "/export/apps/tony/conf";

  public static final String AM_NAME = "am";
  public static final String CHIEF_JOB_NAME = "chief";
  public static final String PS_JOB_NAME = "ps";
  public static final String WORKER_JOB_NAME = "worker";
  public static final String NOTEBOOK_JOB_NAME = "notebook";
  public static final String DRIVER_JOB_NAME = "driver";

  public static final String ATTEMPT_NUMBER = "ATTEMPT_NUMBER";

  public static final String TEST_AM_CRASH = "TEST_AM_CRASH";
  public static final String TEST_WORKER_TERMINATED = "TEST_WORKER_TERMINATION";
  public static final String TEST_TASK_COMPLETION_NOTIFICATION_DELAYED = "TEST_TASK_COMPLETION_NOTIFICATION_DELAYED";
  public static final String TEST_TASK_EXECUTOR_NUM_HB_MISS = "TEST_TASK_EXECUTOR_NUM_HB_MISS";
  // Should be of the form type#id#ms
  public static final String TEST_TASK_EXECUTOR_SKEW = "TEST_TASK_EXECUTOR_SKEW";

  // Used to get all Hadoop jar paths. Reference: https://www.tensorflow.org/deploy/hadoop
  public static final String HADOOP_CLASSPATH_COMMAND = "CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob) ";
  public static final String SKIP_HADOOP_PATH = "SKIP_HADOOP_PATH";

  public static final String TONY_FOLDER = ".tony";

  public static final String TONY_HISTORY_INTERMEDIATE = "intermediate";

  // Configuration related constants
  public static final String APP_TYPE = "TONY";
  // Name of the file containing all configuration keys and their default values
  public static final String TONY_DEFAULT_XML = "tony-default.xml";
  // Default file name of user-provided configuration file
  public static final String TONY_XML = "tony.xml";
  // TonY-internal file name for final configurations, after user-provided configuration
  // file and CLI confs are combined. This file is uploaded to HDFS and localized to containers
  public static final String TONY_FINAL_XML = "tony-final.xml";

  // Module relative path
  public static final String TONY_CORE_SRC = "./tony-core/src/";

  // YARN resources
  public static final String MEMORY = "memory";
  public static final String VCORES = "vcores";
  public static final String GPUS = "gpus";

  // pid environment variable set by YARN
  public static final String JVM_PID = "JVM_PID";

  // Metrics
  public static final String METRICS_RPC_PORT = "METRICS_RPC_PORT";
  public static final String MAX_MEMORY_BYTES = "MAX_MEMORY_BYTES";
  public static final String AVG_MEMORY_BYTES = "AVG_MEMORY_BYTES";
  // Maximum percent of time one or more kernels was executing on GPU
  public static final String MAX_GPU_UTILIZATION = "MAX_GPU_UTILIZATION";
  // Average across GPUs of percent of time one or more kernels was executing on GPU
  public static final String AVG_GPU_UTILIZATION = "AVG_GPU_UTILIZATION";
  // Maximum total percentage GPU on-board frame buffer memory used
  public static final String MAX_GPU_FB_MEMORY_USAGE = "MAX_GPU_FB_MEMORY_USAGE";
  // Average across GPUs of percentage on-board frame buffer memory used
  public static final String AVG_GPU_FB_MEMORY_USAGE = "AVG_GPU_FB_MEMORY_USAGE";
  // Maximum BAR1 (used to map FB for direct access by CPU) memory used
  public static final String MAX_GPU_MAIN_MEMORY_USAGE = "MAX_GPU_MAIN_MEMORY_USAGE";
  // Average across GPUs of BAR1 memory used
  public static final String AVG_GPU_MAIN_MEMORY_USAGE = "AVG_GPU_MAIN_MEMORY_USAGE";

  public static final int MAX_REPEATED_GPU_ERROR_ALLOWED = 10;

  private Constants() { }
}
