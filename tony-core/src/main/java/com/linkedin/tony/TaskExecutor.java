/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.io.HdfsAvroFileSplitReader;
import com.linkedin.tony.rpc.impl.ApplicationRpcClient;
import com.linkedin.tony.util.Utils;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import py4j.GatewayServer;

import static com.linkedin.tony.Constants.CORE_SITE_CONF;
import static com.linkedin.tony.Constants.HADOOP_CONF_DIR;
import static com.linkedin.tony.TonyConfigurationKeys.MLFramework;

/**
 * Content that we want to run in the containers. TaskExecutor will register itself with AM and fetch cluster spec from
 * AM. After the cluster spec is collected, TaskExecutor will set up local environment and start the worker task.
 */
public class TaskExecutor {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

  private static final int MAX_NUM_FAILED_HB_ATTEMPTS = 5;

  @VisibleForTesting
  protected Configuration tonyConf = new Configuration();
  private ServerSocket rpcSocket;
  private int rpcPort;
  private ServerSocket tbSocket;
  private int tbPort;
  private ServerSocket gatewayServerSocket;
  private int gatewayServerPort;
  private int timeOut;
  private String amAddress;
  private String taskCommand;
  private String venv;
  private String clusterSpec;
  private String jobName;
  private int taskIndex;
  private String taskId;
  private int numTasks;
  private Configuration yarnConf = new Configuration();
  private Configuration hdfsConf = new Configuration();
  private ApplicationRpcClient proxy;
  private Map<String, String> shellEnv = new HashMap<>();
  private int hbInterval;
  private final ScheduledExecutorService hbExec = Executors.newScheduledThreadPool(1);
  private int numFailedHBAttempts = 0;
  private MLFramework framework;

  protected TaskExecutor() throws IOException {
    // Reserve a rpcSocket rpcPort.
    this.rpcSocket = new ServerSocket(0);
    this.rpcPort = this.rpcSocket.getLocalPort();
    this.tbSocket = new ServerSocket(0);
    this.tbPort = this.tbSocket.getLocalPort();
    this.gatewayServerSocket = new ServerSocket(0);
    this.gatewayServerPort = this.gatewayServerSocket.getLocalPort();

    LOG.info("Reserved rpcPort: " + this.rpcPort);
    LOG.info("Reserved tbPort: " + this.tbPort);
    LOG.info("Reserved py4j gatewayServerPort: " + this.gatewayServerPort);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("TaskExecutor is running..");
    TaskExecutor executor = new TaskExecutor();
    // Set up py4j
    GatewayServer pyServer = new GatewayServer(executor, executor.gatewayServerPort);
    executor.gatewayServerSocket.close();
    pyServer.start();
    boolean sanitized = executor.init(args);
    if (!sanitized) {
      LOG.fatal("Failed to initialize TaskExecutor.");
      System.exit(-1);
    }

    if (executor.venv != null) {
      LOG.info("Unpacking Python virtual environment: " + executor.venv);
      Utils.unzipArchive(executor.venv, Constants.PYTHON_VENV_DIR);
    } else {
      LOG.info("No virtual environment uploaded.");
    }

    executor.jobName = System.getenv(Constants.JOB_NAME);
    executor.taskIndex = Integer.parseInt(System.getenv(Constants.TASK_INDEX));
    executor.numTasks = Integer.parseInt(System.getenv(Constants.TASK_NUM));
    executor.taskId = executor.jobName + ":" + executor.taskIndex;

    LOG.info("Executor is running task " + executor.jobName + " " + executor.taskIndex);

    executor.clusterSpec = executor.registerAndGetClusterSpec(executor.amAddress);
    if (executor.clusterSpec == null) {
      LOG.error("Failed to register worker with AM.");
      throw new Exception("Failed to register worker with AM.");
    }
    LOG.info("Successfully registered and got cluster spec: " + executor.clusterSpec);

    // Release the rpcPort and start the process
    executor.rpcSocket.close();

    if (executor.taskIndex == 0 && executor.jobName.equals("worker")) {
      executor.registerTensorBoardUrl();
      executor.tbSocket.close();
    }

    // Execute the user command
    HashMap<String, String> extraEnv = new HashMap<>(executor.shellEnv);
    switch (executor.framework) {
      case TENSORFLOW: {
        LOG.info("Setting up TensorFlow jobs..");
        extraEnv.put(Constants.TB_PORT, String.valueOf(executor.tbPort));
        extraEnv.put(Constants.PY4JGATEWAY, String.valueOf(executor.gatewayServerPort));
        extraEnv.put(Constants.JOB_NAME, String.valueOf(executor.jobName));
        extraEnv.put(Constants.TASK_INDEX, String.valueOf(executor.taskIndex));
        extraEnv.put(Constants.CLUSTER_SPEC, String.valueOf(executor.clusterSpec));
        extraEnv.put(Constants.TF_CONFIG, Utils.constructTFConfig(executor.clusterSpec, executor.jobName, executor.taskIndex));
        break;
      }
      case PYTORCH: {
        LOG.info("Setting up PyTorch jobs..");
        String initMethod = Utils.parseClusterSpecForPytorch(executor.clusterSpec);
        if (initMethod == null) {
          System.exit(-1);
        }
        LOG.info("Init method is: " + initMethod);
        extraEnv.put(Constants.INIT_METHOD, String.valueOf(initMethod));
        extraEnv.put(Constants.RANK, String.valueOf(executor.taskIndex));
        extraEnv.put(Constants.WORLD, String.valueOf(executor.numTasks));
        break;
      }
    }

    int exitCode = Utils.executeShell(executor.taskCommand, executor.timeOut, extraEnv);
    // START - worker skew testing:
    executor.skewAndHangIfTesting();
    // END - worker skew testing:
    executor.registerExecutionResult(exitCode, executor.jobName, String.valueOf(executor.taskIndex));

    LOG.info("Child process exited with exit code " + exitCode);
    System.exit(exitCode);
  }

  protected boolean init(String[] args) throws Exception {
    tonyConf.addResource(new Path(Constants.TONY_FINAL_XML));
    Options opts = new Options();
    opts.addOption("am_address", true, "The address to the application master.");
    opts.addOption("task_command", true, "The task command to run.");
    opts.addOption("venv", true, "The name of python venv zip.");
    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
    CommandLine cliParser = new GnuParser().parse(opts, args);
    amAddress = cliParser.getOptionValue("am_address", "");
    taskCommand = cliParser.getOptionValue("task_command", "exit 0");
    timeOut = tonyConf.getInt(TonyConfigurationKeys.WORKER_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_WORKER_TIMEOUT);
    hbInterval = tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS);
    String[] shellEnvs = cliParser.getOptionValues("shell_env");
    shellEnv = Utils.parseKeyValue(shellEnvs);
    LOG.info("Task command: " + taskCommand);
    venv = cliParser.getOptionValue("venv");
    framework = MLFramework.valueOf(
        tonyConf.get(TonyConfigurationKeys.FRAMEWORK_NAME, TonyConfigurationKeys.DEFAULT_FRAMEWORK_NAME).toUpperCase());

    Utils.unzipArchive(Constants.TONY_ZIP_NAME, "./");
    if (System.getenv(Constants.YARN_CONF_PATH) != null) {
      yarnConf.addResource(new Path(System.getenv(Constants.YARN_CONF_PATH)));
    }
    if (System.getenv(Constants.HDFS_CONF_PATH) != null) {
      hdfsConf.addResource(new Path(System.getenv(Constants.HDFS_CONF_PATH)));
    }
    LOG.info("Setting up Rpc client, connecting to: " + amAddress);
    proxy = ApplicationRpcClient.getInstance(amAddress.split(":")[0], Integer.parseInt(amAddress.split(":")[1]), yarnConf);
    return true;
  }

  private String registerAndGetClusterSpec(String amAddress) {
    LOG.info("Application Master address : " + amAddress);
    ContainerId containerId = ContainerId.fromString(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()));
    String hostName = Utils.getCurrentHostName();
    LOG.info("ContainerId is: " + containerId + " HostName is: " + hostName);

    hangIfTesting();

    // Start the Heartbeater..
    hbExec.scheduleAtFixedRate(new Heartbeater(),
        0, hbInterval, TimeUnit.MILLISECONDS);

    LOG.info("Connecting to " + amAddress + " to register worker spec: " + jobName + " " + taskIndex + " "
             + hostName + ":" + rpcPort);
    return Utils.pollTillNonNull(() ->
        proxy.registerWorkerSpec(jobName + ":" + taskIndex,
            hostName + ":" + rpcPort), 3, 0);
  }

  private void registerTensorBoardUrl() {
    String hostName = Utils.getCurrentHostName();
    String tbUrl = hostName + ":" + tbPort;
    LOG.info("TensorBoard address : " + tbUrl);
    String response = Utils.pollTillNonNull(() -> proxy.registerTensorBoardUrl(tbUrl), 1, 60);
    if (response != null) {
      LOG.info("Register TensorBoard response: " + response);
    }
  }

  private void registerExecutionResult(int exitCode, String jobName, String jobIndex) {
    String sessionId = System.getenv(Constants.SESSION_ID);
    String response = Utils.pollTillNonNull(
        () -> proxy.registerExecutionResult(exitCode, jobName, jobIndex, sessionId), 1, 60);
    if (response != null) {
      LOG.info("AM response for result execution run: " + response);
    }
  }

  private class Heartbeater implements Runnable {
    int hbMissCounter = 0;
    int numHbToMiss;

    private Heartbeater() {
      String hbMissStr = System.getenv(Constants.TEST_TASK_EXECUTOR_NUM_HB_MISS);
      try {
        int numMisses = Integer.parseInt(hbMissStr);
        if (numMisses > 0) {
          numHbToMiss = numMisses;
        }
      } catch (Exception e) {
        numHbToMiss = 0;
      }
    }

    @Override
    public void run() {
      try {
        if (hbMissCounter == 0) {
          LOG.debug("[" + taskId + "] Sending Ping !!");
          proxy.taskExecutorHeartbeat(taskId);
          numFailedHBAttempts = 0;
          hbMissCounter = numHbToMiss;
        } else {
          LOG.debug("[" + taskId + "] Skipping heartbeat for Testing !!");
          hbMissCounter--;
        }
      } catch (Exception e) {
        LOG.error("[" + taskId + "] Failed to send Heart Beat.", e);
        if (++numFailedHBAttempts > MAX_NUM_FAILED_HB_ATTEMPTS) {
          LOG.error("[" + taskId + "] Exceeded Failed Heart Beat send attempts.. going to die !!");
          e.printStackTrace();
          System.exit(-1);
        } else {
          LOG.warn("Will retry heartbeat..");
        }
      }
    }
  }

  //region TonyDataFeed

  // TODO : currently requires caller (tf job) to provide the path to read
  // maybe a better abstraction if task executor itself figures this out (if
  // possible at all.)
  @SuppressWarnings("unused")
  public HdfsAvroFileSplitReader getHdfsAvroFileSplitReader(List<String> readPaths)
      throws IOException {
    return getHdfsAvroFileSplitReader(readPaths, true);
  }

  public HdfsAvroFileSplitReader getHdfsAvroFileSplitReader(List<String> readPaths,
                                                            boolean useRandomShuffle)
      throws IOException {
    Configuration hdfsConf = new Configuration();
    hdfsConf.addResource(new Path(
        System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
    return new HdfsAvroFileSplitReader(hdfsConf, readPaths, this.taskIndex,
        this.numTasks, useRandomShuffle);
  }


  //endregion

  //region Testing

  private void hangIfTesting() {
    // Simulate hanging task executor if enabled and is first attempt
    String shouldHang = System.getenv(Constants.TEST_TASK_EXECUTOR_HANG);
    String attempt = System.getenv(Constants.ATTEMPT_NUMBER);
    int attemptNumber = attempt == null ? 0 : Integer.valueOf(attempt);
    if (shouldHang != null && Boolean.parseBoolean(shouldHang) && attemptNumber < 1) {
      LOG.info("Hanging for 20 seconds for testing purposes");
      try {
        Thread.sleep(20000);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted while hanging forever", e);
      }
      // We still exit after 20 seconds to prevent this process from sticking around forever.
      // In the cluster, when using cgroups, when the container for this process is killed, this process will also be
      // killed, but when using MiniYARNCluster, that's not the case, so this process still needs to exit during tests.
      System.exit(-1);
    }
  }

  private void skewAndHangIfTesting() {
    String skewInstr = System.getenv(Constants.TEST_TASK_EXECUTOR_SKEW);
    if (skewInstr != null) {
      String[] instr = skewInstr.split("#");
      try {
        if (instr.length == 3 && instr[0].equals(this.jobName)
            && Integer.parseInt(instr[1]) == this.taskIndex) {
          int waitTime = Integer.parseInt(instr[2]);
          LOG.info("Will sleep for [" + waitTime + "] as instructed to simulate skew");
          try {
            Thread.sleep(waitTime);
          } catch (InterruptedException e) {
            LOG.error("Thread interrupted while hanging..", e);
          }
        }
      } catch (Exception e) {
        // Could be due to a parsing Exception...
        LOG.error("Got Exception while parsing skew instruction", e);
      }
    }
  }
  //endregion

}
