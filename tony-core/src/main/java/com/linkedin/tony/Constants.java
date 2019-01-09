/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class Constants {
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
  public static final String PY4JGATEWAY = "PY4J_GATEWAY_PORT";

  // Environment variables for resource localization
  public static final String TONY_CONF_PREFIX = "TONY_CONF";

  public static final String PATH_SUFFIX = "_PATH";
  public static final String TIMESTAMP_SUFFIX = "_TIMESTAMP";
  public static final String LENGTH_SUFFIX = "_LENGTH";

  public static final String TONY_SRC_ZIP_NAME = "tony_src.zip";

  public static final String PYTHON_VENV_ZIP = "venv.zip";
  public static final String PYTHON_VENV_DIR = "venv";
  public static final String TASK_PARAM_KEY = "MODEL_PARAMS";

  public static final String AM_STDOUT_FILENAME = "amstdout.log";
  public static final String AM_STDERR_FILENAME = "amstderr.log";

  public static final String HDFS_SITE_CONF = "hdfs-site.xml";
  public static final String YARN_SITE_CONF = "yarn-site.xml";
  public static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
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
  public static final String TEST_TASK_EXECUTOR_HANG = "TEST_TASK_EXECUTOR_HANG";
  public static final String TEST_TASK_EXECUTOR_NUM_HB_MISS = "TEST_TASK_EXECUTOR_NUM_HB_MISS";
  // Should be of the form type#id#ms
  public static final String TEST_TASK_EXECUTOR_SKEW = "TEST_TASK_EXECUTOR_SKEW";

  // Used to get all Hadoop jar paths. Reference: https://www.tensorflow.org/deploy/hadoop
  public static final String HADOOP_CLASSPATH_COMMAND = "CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob) ";
  public static final String SKIP_HADOOP_PATH = "SKIP_HADOOP_PATH";

  public static final String TONY_FOLDER = ".tony";

  public static final String TONY_HISTORY_INTERMEDIATE = "intermediate";

  // Configuration related constants
  // Name of the file containing all configuration keys and their default values
  public static final String TONY_DEFAULT_XML = "tony-default.xml";
  // Default file name of user-provided configuration file
  public static final String TONY_XML = "tony.xml";
  // TonY-internal file name for final configurations, after user-provided configuration
  // file and CLI confs are combined. This file is uploaded to HDFS and localized to containers
  public static final String TONY_FINAL_XML = "tony-final.xml";

  // Module relative path
  public static final String TONY_CORE_SRC = "./tony-core/src/";

  private Constants() { }
}
