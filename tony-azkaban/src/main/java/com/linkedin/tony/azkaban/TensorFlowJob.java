/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.azkaban;

import azkaban.flow.CommonJobProperties;
import azkaban.jobtype.HadoopJavaJob;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;


/**
 * The Azkaban jobtype for running a TensorFlow job.
 * This class is used by Azkaban executor to build the classpath, main args,
 * env and jvm properties.
 */
public class TensorFlowJob extends HadoopJavaJob {
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;
  private File tokenFile = null;
  public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
  public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
  public static final String WORKER_ENV_PREFIX = "worker_env.";
  private static final String TONY_XML = "_tony-conf/tony.xml";
  private static final String TONY_CONF_PREFIX = "tony.";
  private HadoopSecurityManager hadoopSecurityManager;
  private File tonyConfFile;

  public TensorFlowJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);

    getJobProps().put(CommonJobProperties.JOB_ID, jobid);
    shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING, Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);

    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to get hadoop security manager!" + e.getCause());
      }
    }

    tonyConfFile = new File(getWorkingDirectory(), TONY_XML);
  }

  @Override
  protected List<String> getClassPaths() {
    List<String> classPath = super.getClassPaths();
    classPath.add(tonyConfFile.getParent());
    return classPath;
  }

  @Override
  public void run() throws Exception {
    getLog().info("Hello world from TensorFlow!");
    setupHadoopOpts(getJobProps());
    if (shouldProxy && obtainTokens) {
      getLog().info("Need to proxy. Getting tokens.");
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());

      tokenFile = HadoopJobUtils.getHadoopTokens(hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }
    super.run();
  }

  private void setupHadoopOpts(Props props) {
    if (props.containsKey(HADOOP_GLOBAL_OPTS)) {
      String hadoopGlobalOps = props.getString(HADOOP_GLOBAL_OPTS);
      if (props.containsKey(HADOOP_OPTS)) {
        String hadoopOps = props.getString(HADOOP_OPTS);
        props.put(HADOOP_OPTS, String.format("%s %s", hadoopOps, hadoopGlobalOps));
      } else {
        props.put(HADOOP_OPTS, hadoopGlobalOps);
      }
    }
  }

  @Override
  public String getJavaClass() {
    return "com.linkedin.tony.TonyClient";
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    String typeUserGlobalJVMArgs = getJobProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
    if (typeUserGlobalJVMArgs != null) {
      args += " " + typeUserGlobalJVMArgs;
    }
    String typeSysGlobalJVMArgs = getSysProps().getString(HadoopJobUtils.JOBTYPE_GLOBAL_JVM_ARGS, null);
    if (typeSysGlobalJVMArgs != null) {
      args += " " + typeSysGlobalJVMArgs;
    }
    String typeUserJVMArgs = getJobProps().getString(HadoopJobUtils.JOBTYPE_JVM_ARGS, null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs;
    }
    String typeSysJVMArgs = getSysProps().getString(HadoopJobUtils.JOBTYPE_JVM_ARGS, null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs;
    }

    String typeUserJVMArgs2 = getJobProps().getString(HadoopJobUtils.JVM_ARGS, null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs2;
    }
    String typeSysJVMArgs2 = getSysProps().getString(HadoopJobUtils.JVM_ARGS, null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs2;
    }
    return args;
  }

  @Override
  protected String getMainArguments() {
    String args = super.getMainArguments();
    info("All job props: " + getJobProps());
    info("All sys props: " + getSysProps());
    String srcDir = getJobProps().getString(TensorFlowJobArg.SRC_DIR.azPropName, "src");
    args += " " + TensorFlowJobArg.SRC_DIR.tfParamName + " " + srcDir;

    String hdfsClasspath = getJobProps().getString(TensorFlowJobArg.HDFS_CLASSPATH.azPropName, null);
    if (hdfsClasspath != null) {
      args += " " + TensorFlowJobArg.HDFS_CLASSPATH.tfParamName + " " + hdfsClasspath;
    }

    Map<String, String> workerEnvs = getJobProps().getMapByPrefix(WORKER_ENV_PREFIX);
    for (Map.Entry<String, String> workerEnv : workerEnvs.entrySet()) {
      args += " " + TensorFlowJobArg.SHELL_ENV.tfParamName + " " + workerEnv.getKey() + "=" + workerEnv.getValue();
    }

    String taskParams = getJobProps().getString(TensorFlowJobArg.TASK_PARAMS.azPropName, null);
    if (taskParams != null) {
      args += " " + TensorFlowJobArg.TASK_PARAMS.tfParamName + " '" + taskParams + "'";
    }

    String pythonBinaryPath = getJobProps().getString(TensorFlowJobArg.PYTHON_BINARY_PATH.azPropName, null);
    if (pythonBinaryPath != null) {
      args += " " + TensorFlowJobArg.PYTHON_BINARY_PATH.tfParamName + " " + pythonBinaryPath;
    }

    String pythonVenv = getJobProps().getString(TensorFlowJobArg.PYTHON_VENV.azPropName, null);
    if (pythonVenv != null) {
      args += " " + TensorFlowJobArg.PYTHON_VENV.tfParamName + " " + pythonVenv;
    }

    String executes = getJobProps().getString(TensorFlowJobArg.EXECUTES.azPropName, null);
    if (executes != null) {
      args += " " + TensorFlowJobArg.EXECUTES.tfParamName + " " + executes;
    }

    Map<String, String> tfConfs = getJobProps().getMapByPrefix(TONY_CONF_PREFIX);
    Configuration tfConf = new Configuration(false);
    for (Map.Entry<String, String> tfConfEntry : tfConfs.entrySet()) {
      tfConf.set(TONY_CONF_PREFIX + tfConfEntry.getKey(), tfConfEntry.getValue());
    }

    // Write user's tony confs to an xml to be localized.
    tonyConfFile.getParentFile().mkdir();
    try (OutputStream os = new FileOutputStream(tonyConfFile)) {
      tfConf.writeXml(os);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create " + TONY_XML + " conf file. Exiting.", e);
    }

    info("Complete main arguments: " + args);

    return args;
  }
}
