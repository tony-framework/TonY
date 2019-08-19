/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.azkaban;

import azkaban.flow.CommonJobProperties;
import azkaban.jobtype.HadoopJavaJob;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


/**
 * The Azkaban jobtype for running a TonY job.
 * This class is used by Azkaban executor to build the classpath, main args,
 * env and jvm properties.
 */
public class TonyJob extends HadoopJavaJob {
  public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
  public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
  public static final String WORKER_ENV_PREFIX = "worker_env.";
  private static final String TONY_CONF_PREFIX = "tony.";
  public static final String TONY_APPLICATION_TAGS =
      TONY_CONF_PREFIX + "application.tags";
  private String tonyXml;
  private File tonyConfFile;
  private final Configuration tonyConf;

  public TonyJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);

    tonyXml = String.format("_tony-conf-%s/tony.xml", jobid);
    tonyConfFile = new File(getWorkingDirectory(), tonyXml);
    tonyConf = getJobConfiguration();
  }

  private Configuration getJobConfiguration() {
    Map<String, String> tonyConfs = getJobProps().getMapByPrefix(TONY_CONF_PREFIX);
    Configuration tonyConf = new Configuration(false);
    for (Map.Entry<String, String> confEntry : tonyConfs.entrySet()) {
      tonyConf.set(TONY_CONF_PREFIX + confEntry.getKey(), confEntry.getValue());
    }

    // pass flow information to the tony job through configuration
    String[] tagKeys = new String[] { CommonJobProperties.EXEC_ID,
        CommonJobProperties.FLOW_ID, CommonJobProperties.PROJECT_NAME };
    String applicationTags =
        HadoopJobUtils.constructHadoopTags(getJobProps(), tagKeys);
    tonyConf.set(TONY_APPLICATION_TAGS, applicationTags);
    return tonyConf;
  }

  public Configuration getTonyJobConf() {
    return tonyConf;
  }

  @Override
  protected List<String> getClassPaths() {
    List<String> classPath = super.getClassPaths();
    classPath.add(tonyConfFile.getParent());
    return classPath;
  }

  @Override
  public void run() throws Exception {
    getLog().info("Running TonY job!");
    setupHadoopOpts(getJobProps());
    setupJobConfigurationFile();
    super.run();
  }

  private void setupJobConfigurationFile() throws IOException {
    // Write user's tony confs to an xml to be localized.
    File parentDir = tonyConfFile.getParentFile();
    if (!parentDir.mkdirs() && !parentDir.exists()) {
      throw new IOException("Failed to create parent directory " + tonyConfFile.getParentFile()
          + " for TonY conf file.");
    }
    try (OutputStream os = new FileOutputStream(tonyConfFile)) {
      tonyConf.writeXml(os);
    } catch (IOException e) {
      throw new IOException("Failed to create " + tonyXml + " conf file. Exiting.", e);
    }
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
    String srcDir = getJobProps().getString(TonyJobArg.SRC_DIR.azPropName, "src");
    args.append(" " + TonyJobArg.SRC_DIR.tonyParamName + " " + srcDir);

    String hdfsClasspath = getJobProps().getString(TonyJobArg.HDFS_CLASSPATH.azPropName, null);
    if (hdfsClasspath != null) {
      args.append(" " + TonyJobArg.HDFS_CLASSPATH.tonyParamName + " " + hdfsClasspath);
    }

    Map<String, String> workerEnvs = getJobProps().getMapByPrefix(WORKER_ENV_PREFIX);
    for (Map.Entry<String, String> workerEnv : workerEnvs.entrySet()) {
      args.append(" " + TonyJobArg.SHELL_ENV.tonyParamName + " " + workerEnv.getKey()
          + "=" + workerEnv.getValue());
    }

    String taskParams = getJobProps().getString(TonyJobArg.TASK_PARAMS.azPropName, null);
    if (taskParams != null) {
      args.append(" " + TonyJobArg.TASK_PARAMS.tonyParamName + " '" + taskParams + "'");
    }

    String pythonBinaryPath = getJobProps().getString(TonyJobArg.PYTHON_BINARY_PATH.azPropName, null);
    if (pythonBinaryPath != null) {
      args.append(" " + TonyJobArg.PYTHON_BINARY_PATH.tonyParamName + " " + pythonBinaryPath);
    }

    String pythonVenv = getJobProps().getString(TonyJobArg.PYTHON_VENV.azPropName, null);
    if (pythonVenv != null) {
      args.append(" " + TonyJobArg.PYTHON_VENV.tonyParamName + " " + pythonVenv);
    }

    String executes = getJobProps().getString(TonyJobArg.EXECUTES.azPropName, null);
    if (executes != null) {
      args.append(" " + TonyJobArg.EXECUTES.tonyParamName + " " + executes);
    }

    info("Complete main arguments: " + args);

    return args.toString();
  }
}
