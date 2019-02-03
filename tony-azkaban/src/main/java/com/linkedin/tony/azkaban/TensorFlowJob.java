/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.azkaban;

import azkaban.jobtype.HadoopJavaJob;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * The Azkaban jobtype for running a TensorFlow job.
 * This class is used by Azkaban executor to build the classpath, main args,
 * env and jvm properties.
 */
public class TensorFlowJob extends HadoopJavaJob {
  public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
  public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
  public static final String WORKER_ENV_PREFIX = "worker_env.";
  private static final String TONY_CONF_PREFIX = "tony.";
  private String tonyXml;
  private File tonyConfFile;

  public TensorFlowJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);

    tonyXml = String.format("_tony-conf-%s/tony.xml", jobid);
    tonyConfFile = new File(getWorkingDirectory(), tonyXml);
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
    super.run();
  }

  private void setupHadoopOpts(Props props) {
    if (props.containsKey(HADOOP_GLOBAL_OPTS)) {
      String hadoopGlobalOpts = props.getString(HADOOP_GLOBAL_OPTS);
      if (props.containsKey(HADOOP_OPTS)) {
        String hadoopOpts = props.getString(HADOOP_OPTS);
        props.put(HADOOP_OPTS, String.format("%s %s", hadoopOpts, hadoopGlobalOpts));
      } else {
        props.put(HADOOP_OPTS, hadoopGlobalOpts);
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

    String userJVMArgs = getJobProps().getString(HadoopJobUtils.JVM_ARGS, null);
    if (userJVMArgs != null) {
      args += " " + userJVMArgs;
    }
    String sysJVMArgs = getSysProps().getString(HadoopJobUtils.JVM_ARGS, null);
    if (sysJVMArgs != null) {
      args += " " + sysJVMArgs;
    }
    return args;
  }

  @Override
  protected String getMainArguments() {
    StringBuilder args = new StringBuilder(super.getMainArguments());
    info("All job props: " + getJobProps());
    info("All sys props: " + getSysProps());
    String srcDir = getJobProps().getString(TensorFlowJobArg.SRC_DIR.azPropName, "src");
    args.append(" " + TensorFlowJobArg.SRC_DIR.tonyParamName + " " + srcDir);

    String hdfsClasspath = getJobProps().getString(TensorFlowJobArg.HDFS_CLASSPATH.azPropName, null);
    if (hdfsClasspath != null) {
      args.append(" " + TensorFlowJobArg.HDFS_CLASSPATH.tonyParamName + " " + hdfsClasspath);
    }

    Map<String, String> workerEnvs = getJobProps().getMapByPrefix(WORKER_ENV_PREFIX);
    for (Map.Entry<String, String> workerEnv : workerEnvs.entrySet()) {
      args.append(" " + TensorFlowJobArg.SHELL_ENV.tonyParamName + " " + workerEnv.getKey()
          + "=" + workerEnv.getValue());
    }

    String taskParams = getJobProps().getString(TensorFlowJobArg.TASK_PARAMS.azPropName, null);
    if (taskParams != null) {
      args.append(" " + TensorFlowJobArg.TASK_PARAMS.tonyParamName + " '" + taskParams + "'");
    }

    String pythonBinaryPath = getJobProps().getString(TensorFlowJobArg.PYTHON_BINARY_PATH.azPropName, null);
    if (pythonBinaryPath != null) {
      args.append(" " + TensorFlowJobArg.PYTHON_BINARY_PATH.tonyParamName + " " + pythonBinaryPath);
    }

    String pythonVenv = getJobProps().getString(TensorFlowJobArg.PYTHON_VENV.azPropName, null);
    if (pythonVenv != null) {
      args.append(" " + TensorFlowJobArg.PYTHON_VENV.tonyParamName + " " + pythonVenv);
    }

    String executes = getJobProps().getString(TensorFlowJobArg.EXECUTES.azPropName, null);
    if (executes != null) {
      args.append(" " + TensorFlowJobArg.EXECUTES.tonyParamName + " " + executes);
    }

    Map<String, String> tonyConfs = getJobProps().getMapByPrefix(TONY_CONF_PREFIX);
    Configuration tfConf = new Configuration(false);
    for (Map.Entry<String, String> tfConfEntry : tonyConfs.entrySet()) {
      tfConf.set(TONY_CONF_PREFIX + tfConfEntry.getKey(), tfConfEntry.getValue());
    }

    // Write user's tony confs to an xml to be localized.
    if (!tonyConfFile.getParentFile().mkdir()) {
      throw new RuntimeException("Failed to create parent directory for TonY conf file.");
    }
    try (OutputStream os = new FileOutputStream(tonyConfFile)) {
      tfConf.writeXml(os);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create " + tonyXml + " conf file. Exiting.", e);
    }

    info("Complete main arguments: " + args);

    return args.toString();
  }
}
