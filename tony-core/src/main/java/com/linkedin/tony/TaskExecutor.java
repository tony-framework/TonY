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
import static com.linkedin.tony.TonyConfigurationKeys.MLFramework;

/**
 * Content that we want to run in the containers. TaskExecutor will register itself with AM and fetch cluster spec from
 * AM. After the cluster spec is collected, TaskExecutor will set up local environment and start the worker task.
 */
public class TaskExecutor {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

  private static final int MAX_NUM_FAILED_HB_ATTEMPTS = 5;

  @VisibleForTesting
  protected Configuration tonyConf = new Configuration(false);
  private ServerSocket rpcSocket;
  private int rpcPort;
  private ServerSocket tbSocket;
  private int tbPort;
  private ServerSocket gatewayServerSocket;
  private int gatewayServerPort;
  private int timeOut;
  private String amAddress;
  private String taskCommand;
  private String clusterSpec;
  private String jobName;
  private int taskIndex;
  private String taskId;
  private int numTasks;
  private boolean isChief;
  private Configuration yarnConf = new Configuration(false);
  private Configuration hdfsConf = new Configuration(false);
  private ApplicationRpcClient proxy;
  private Map<String, String> shellEnv = new HashMap<>();
  private int hbInterval;
  private final ScheduledExecutorService hbExec = Executors.newScheduledThreadPool(1);
  private int numFailedHBAttempts = 0;
  private MLFramework framework;

  protected TaskExecutor() { }

  private void setupTaskExecutor() {
    jobName = System.getenv(Constants.JOB_NAME);
    taskCommand = tonyConf.get(TonyConfigurationKeys.getExecuteCommandKey(jobName),
            tonyConf.get(TonyConfigurationKeys.getContainerExecuteCommandKey(), "exit -1"));
    taskIndex = Integer.parseInt(System.getenv(Constants.TASK_INDEX));
    numTasks = Integer.parseInt(System.getenv(Constants.TASK_NUM));
    taskId = jobName + ":" + taskIndex;

    String isChiefEnvValue = System.getenv(Constants.IS_CHIEF);
    isChief = Boolean.parseBoolean(isChiefEnvValue);

    LOG.info("Executor is running task " + jobName + " " + taskIndex);
  }

  /**
   * We bind to random ports and then release them, and these are the ports used by the task.
   * However, there is the possibility that another process grabs the port between when it's released and used again.
   */
  private void setupPorts() throws IOException {
    // Reserve a rpcSocket rpcPort.
    this.rpcSocket = new ServerSocket(0);
    this.rpcPort = this.rpcSocket.getLocalPort();
    this.rpcSocket.close();
    LOG.info("Reserved rpcPort: " + this.rpcPort);

    this.gatewayServerSocket = new ServerSocket(0);
    this.gatewayServerPort = this.gatewayServerSocket.getLocalPort();
    this.gatewayServerSocket.close();
    LOG.info("Reserved py4j gatewayServerPort: " + this.gatewayServerPort);

    // With Estimator API, there is a separate lone "chief" task that runs TensorBoard.
    // With the low-level distributed API, worker 0 runs TensorBoard.
    if (isChief) {
      this.tbSocket = new ServerSocket(0);
      this.tbPort = this.tbSocket.getLocalPort();
      this.tbSocket.close();
      this.registerTensorBoardUrl();
      this.shellEnv.put(Constants.TB_PORT, String.valueOf(this.tbPort));
      LOG.info("Reserved tbPort: " + this.tbPort);
    }
  }

  public static void main(String[] args) throws Exception {
    LOG.info("TaskExecutor is running..");
    TaskExecutor executor = new TaskExecutor();

    executor.initConfigs(args);
    executor.setupTaskExecutor();

    LOG.info("Setting up Rpc client, connecting to: " + executor.amAddress);
    executor.proxy = ApplicationRpcClient.getInstance(executor.amAddress.split(":")[0],
        Integer.parseInt(executor.amAddress.split(":")[1]), executor.yarnConf);

    executor.setupPorts();

    if (new File(Constants.TONY_SRC_ZIP_NAME).exists()) {
      LOG.info("Unpacking src directory..");
      Utils.unzipArchive(Constants.TONY_SRC_ZIP_NAME, "./");
    }
    if (new File(Constants.PYTHON_VENV_ZIP).exists()) {
      LOG.info("Unpacking Python virtual environment.. ");
      Utils.unzipArchive(Constants.PYTHON_VENV_ZIP, Constants.PYTHON_VENV_DIR);
    } else {
      LOG.info("No virtual environment uploaded.");
    }

    executor.clusterSpec = executor.registerAndGetClusterSpec(executor.amAddress);
    if (executor.clusterSpec == null) {
      LOG.error("Failed to register worker with AM.");
      throw new Exception("Failed to register worker with AM.");
    }
    LOG.info("Successfully registered and got cluster spec: " + executor.clusterSpec);

    switch (executor.framework) {
      case TENSORFLOW: {
        LOG.info("Setting up TensorFlow job...");
        // Set up py4j
        GatewayServer pyServer = new GatewayServer(executor, executor.gatewayServerPort);
        pyServer.start();
        executor.shellEnv.put(Constants.PY4JGATEWAY, String.valueOf(executor.gatewayServerPort));
        executor.shellEnv.put(Constants.JOB_NAME, String.valueOf(executor.jobName));
        executor.shellEnv.put(Constants.TASK_INDEX, String.valueOf(executor.taskIndex));
        executor.shellEnv.put(Constants.CLUSTER_SPEC, String.valueOf(executor.clusterSpec));
        executor.shellEnv.put(Constants.TF_CONFIG, Utils.constructTFConfig(executor.clusterSpec, executor.jobName, executor.taskIndex));
        break;
      }
      case PYTORCH: {
        LOG.info("Setting up PyTorch job...");
        String initMethod = Utils.parseClusterSpecForPytorch(executor.clusterSpec);
        if (initMethod == null) {
          System.exit(-1);
        }
        LOG.info("Init method is: " + initMethod);
        executor.shellEnv.put(Constants.INIT_METHOD, initMethod);
        executor.shellEnv.put(Constants.RANK, String.valueOf(executor.taskIndex));
        executor.shellEnv.put(Constants.WORLD, String.valueOf(executor.numTasks));
        break;
      }
      default:
        throw new RuntimeException("Unsupported executor framework: " + executor.framework);
    }

    int exitCode = Utils.executeShell(executor.taskCommand, executor.timeOut, executor.shellEnv);
    // START - worker skew testing:
    executor.skewAndHangIfTesting();
    // END - worker skew testing:
    executor.registerExecutionResult(exitCode, executor.jobName, String.valueOf(executor.taskIndex));

    LOG.info("Child process exited with exit code " + exitCode);
    System.exit(exitCode);
  }

  protected void initConfigs(String[] args) throws Exception {
    tonyConf.addResource(new Path(Constants.TONY_FINAL_XML));
    Options opts = new Options();
    opts.addOption("am_address", true, "The address to the application master.");
    CommandLine cliParser = new GnuParser().parse(opts, args);
    amAddress = cliParser.getOptionValue("am_address", "");
    timeOut = tonyConf.getInt(TonyConfigurationKeys.WORKER_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_WORKER_TIMEOUT);
    hbInterval = tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS);
    String[] shellEnvs = tonyConf.getStrings(TonyConfigurationKeys.EXECUTION_ENV);
    shellEnv = Utils.parseKeyValue(shellEnvs);
    LOG.info("Task command: " + taskCommand);
    framework = MLFramework.valueOf(
        tonyConf.get(TonyConfigurationKeys.FRAMEWORK_NAME, TonyConfigurationKeys.DEFAULT_FRAMEWORK_NAME).toUpperCase());

    Utils.initYarnConf(yarnConf);
    Utils.initHdfsConf(hdfsConf);
  }

  private String registerAndGetClusterSpec(String amAddress) {
    LOG.info("Application Master address : " + amAddress);
    ContainerId containerId = ContainerId.fromString(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()));
    String hostName = Utils.getCurrentHostName();
    LOG.info("ContainerId is: " + containerId + " HostName is: " + hostName);

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
          LOG.error("[" + taskId + "] Exceeded max number of allowed failed heart beat send attempts. "
              + "Going to stop heartbeating!");
          e.printStackTrace();
          throw new RuntimeException(e);
        } else {
          LOG.warn("Will retry heartbeat..");
        }
      }
    }
  }

  // Start region TonyDataFeed

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
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.addResource(new Path(CORE_SITE_CONF));
    return new HdfsAvroFileSplitReader(hdfsConf, readPaths, this.taskIndex,
        this.numTasks, useRandomShuffle);
  }
  // End region TonyDataFeed

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
}
