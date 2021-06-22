/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.impl.ApplicationRpcClient;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.TonyConfigurationKeys.FrameworkType;
import static java.util.Objects.requireNonNull;

/**
 * Content that we want to run in the containers. TaskExecutor will register itself with AM and fetch cluster spec from
 * AM. After the cluster spec is collected, TaskExecutor will set up local environment and start the worker task.
 */
public class TaskExecutor {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

  private static final int MAX_NUM_FAILED_HB_ATTEMPTS = 5;

  @VisibleForTesting
  protected Configuration tonyConf = new Configuration(false);

  private ServerPort rpcPort;
  private ServerPort tbPort;

  private int timeOut;
  private String amHost;
  private int amPort;

  private MetricsRpc metricsProxy;
  private int metricsRPCPort;
  private int metricsIntervalMs;

  private String taskCommand;
  private String clusterSpec;
  private String jobName;
  private int taskIndex;
  private String taskId;
  private int numTasks;
  private boolean isChief;
  private TonyConfigurationKeys.DistributedMode distributedMode;
  private Configuration yarnConf = new Configuration(false);
  private Configuration hdfsConf = new Configuration(false);
  private ApplicationRpcClient proxy;
  private Map<String, String> shellEnv = new HashMap<>();
  private int hbInterval;
  private final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(2);
  private int numFailedHBAttempts = 0;
  private FrameworkType framework;
  private String appIdString;

  private static FrameworkRuntime frameworkRuntime;

  @VisibleForTesting
  public TaskExecutor() { }

  private ServerPort allocatePort(boolean isReusingPort) throws IOException {
    // To prevent other process grabbing the reserved port between releasing the
    // port{@link #releasePorts()} and task command process {@link #taskCommand} starts, task
    // executor reserves the port with port reuse enabled on user's request. When port reuse
    // is enabled, other process can grab the same port only when port reuse is turned on when
    // creating the port.
    return isReusingPort ? ReusablePort.create() : EphemeralPort.create();
  }

  /**
   * We bind to random ports.
   */
  private void setupPorts() throws IOException {
    // Reserve a rpcPort.
    this.rpcPort = requireNonNull(allocatePort(this.isTFGrpcReusingPort()));
    LOG.info("Reserved rpcPort: " + this.rpcPort.getPort());
    // With Estimator API, there is a separate lone "chief" task that runs TensorBoard.
    // With the low-level distributed API, worker 0 runs TensorBoard.
    if (frameworkRuntime.needReserveTBPort()) {
      this.tbPort = requireNonNull(allocatePort(this.isTBServerReusingPort()));
      this.registerTensorBoardUrl();
      this.shellEnv.put(Constants.TB_PORT, String.valueOf(this.tbPort.getPort()));
      LOG.info("Reserved tbPort: " + this.tbPort.getPort());
    }
  }

  private void releasePort(ServerPort port) throws Exception {
    if (port != null) {
      port.close();
    }
  }

  /**
   * Releases the reserved ports if any. This method has to be invoked after ports are created.
   */
  private void releasePorts() throws Exception {
    try {
      this.releasePort(this.rpcPort);
    } finally {
      this.releasePort(this.tbPort);
    }
  }

  /**
   * @return true if reusing port is enabled by user, false otherwise.
   */
  private boolean isTFGrpcReusingPort() {
    // TF_GRPC_REUSE_PORT corresponds to the environment variable defined in tensorflow, check
    // https://github.com/tensorflow/tensorflow/pull/38705 for more details.

    // Why is port reuse optional to users?
    // - Port reuse in TF is only supported in TF 2.3+. User jobs might run with older TF
    //   versions with no port reuse feature.

    // Why is port reuse false by default?
    // - If port reuse is true by default, it requires users working with tensorflow version without
    //   port reuse support to disable port reuse explicitly, otherwise tensorflow job will fail due
    //   to binding to the same port. Given as of now(2020/08) only TF 2.3 supports port reuse,
    //   this option would require more change from users than otherwise, which is more risky
    //   and thus less preferable.
    return this.shellEnv.getOrDefault("TF_GRPC_REUSE_PORT", "false").equalsIgnoreCase("true");
  }

  /**
   * @return true if reusing port is enabled by user, false otherwise.
   */
  private boolean isTBServerReusingPort() {
    // TensorBoard support reuse_port option in TB 2.5.0+. User jobs might run with older TB versions
    // with no port reuse feature.
    // check https://github.com/tensorflow/tensorboard/pull/4616 for more details.

    // How to use it in user code?
    // - Check the TB version >= 2.5.0.
    // - Acoording to the TB_SERVER_REUSE_PORT environment variable, when it is true, user can use the
    //   reuse_port option.
    return this.shellEnv.getOrDefault("TB_SERVER_REUSE_PORT", "false").equalsIgnoreCase("true");
  }

  private static TaskExecutor createExecutor() throws Exception {
    TaskExecutor executor = new TaskExecutor();
    executor.initConfigs();
    Utils.extractResources(executor.appIdString);

    LOG.info("Setting up application RPC client, connecting to: " + executor.amHost + ":" + executor.amPort);
    executor.proxy = ApplicationRpcClient.getInstance(executor.amHost, executor.amPort, executor.yarnConf);

    LOG.info("Setting up metrics RPC client, connecting to: " + executor.amHost + ":" + executor.metricsRPCPort);
    executor.metricsProxy = RPC.getProxy(MetricsRpc.class, RPC.getProtocolVersion(MetricsRpc.class),
        new InetSocketAddress(executor.amHost, executor.metricsRPCPort), executor.yarnConf);
    executor.scheduledThreadPool.scheduleAtFixedRate(
        new TaskMonitor(executor.jobName, executor.taskIndex, executor.yarnConf, executor.tonyConf, executor.metricsProxy),
        0,
        executor.metricsIntervalMs,
        TimeUnit.MILLISECONDS);

    assert frameworkRuntime == null;
    frameworkRuntime = FrameworkRuntime.get(executor.framework);
    frameworkRuntime.initTaskExecutorResource(executor);

    executor.setupPorts();

    executor.clusterSpec = executor.registerAndGetClusterSpec();

    if (executor.clusterSpec == null) {
      LOG.error("Failed to register worker with AM.");
      throw new Exception("Failed to register worker with AM.");
    }
    LOG.debug("Task is on distributed mode: " + executor.distributedMode);
    LOG.info("Successfully registered and got cluster spec: " + executor.clusterSpec);

    return executor;
  }

  public static void main(String[] unused) throws Exception {
    LOG.info("TaskExecutor is running..");
    TaskExecutor executor = null;
    try {
      executor = requireNonNull(createExecutor());
    } catch (Exception ex) {
      if (executor != null) {
        LOG.info("Failed to create TaskExecutor, releasing any reserved ports.");
        executor.releasePorts();
      }
      throw ex;
    }

    // If not reusing port, then reserve them up until before the underlying TF process is
    // launched. See <a href="https://github.com/linkedin/TonY/issues/365">this issue</a> for
    // details.
    if (executor != null) {
      if (!executor.isTFGrpcReusingPort()) {
        LOG.info("Releasing reserved RPC port before launching tensorflow process.");
        executor.releasePort(executor.rpcPort);
      }

      if (!executor.isTBServerReusingPort()) {
        LOG.info("Releasing reserved TB port before launching tensorflow process.");
        executor.releasePort(executor.tbPort);
      }
    }

    int exitCode;
    try {
      exitCode = frameworkRuntime.run();
      // START - worker skew testing:
      executor.skewAndHangIfTesting();
      // END - worker skew testing:
      executor.registerExecutionResult(exitCode, executor.jobName, String.valueOf(executor.taskIndex));
    } finally {
      if (executor.isTFGrpcReusingPort()) {
        LOG.info("TensorFlow process exited, releasing reserved RPC port.");
        executor.releasePort(executor.rpcPort);
      }

      if (executor.isTBServerReusingPort()) {
        LOG.info("Tensorflow process exited, releasing reserved TB port.");
        executor.releasePort(executor.tbPort);
      }
    }

    LOG.info("Child process exited with exit code " + exitCode);
    System.exit(exitCode);
  }

  protected void initConfigs() {
    jobName = System.getenv(Constants.JOB_NAME);
    appIdString = System.getenv(Constants.APPID);
    taskIndex = Integer.parseInt(System.getenv(Constants.TASK_INDEX));
    numTasks = Integer.parseInt(System.getenv(Constants.TASK_NUM));
    taskId = jobName + ":" + taskIndex;
    LOG.info("Executor is running task " + taskId);

    String isChiefEnvValue = System.getenv(Constants.IS_CHIEF);
    isChief = Boolean.parseBoolean(isChiefEnvValue);

    String distributedModeEnvValue = System.getenv(Constants.DISTRIBUTED_MODE_NAME);
    distributedMode = TonyConfigurationKeys.DistributedMode.valueOf(distributedModeEnvValue.toUpperCase());

    amHost = System.getenv(Constants.AM_HOST);
    amPort = Integer.parseInt(System.getenv(Constants.AM_PORT));

    tonyConf.addResource(new Path(Constants.TONY_FINAL_XML));
    timeOut = tonyConf.getInt(TonyConfigurationKeys.WORKER_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_WORKER_TIMEOUT);
    hbInterval = tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS);
    String[] shellEnvs = tonyConf.getStrings(TonyConfigurationKeys.EXECUTION_ENV);
    shellEnv = Utils.parseKeyValue(shellEnvs);
    taskCommand = tonyConf.get(TonyConfigurationKeys.getExecuteCommandKey(jobName),
        tonyConf.get(TonyConfigurationKeys.getContainerExecuteCommandKey()));
    if (taskCommand == null) {
      LOG.fatal("Task command is empty. Please set tony.[jobtype].command "
          + "or pass --executes in command line");
      throw new IllegalArgumentException("Task command is empty.");
    }
    LOG.info("Task command: " + taskCommand);
    framework = FrameworkType.valueOf(
        tonyConf.get(TonyConfigurationKeys.FRAMEWORK_NAME, TonyConfigurationKeys.DEFAULT_FRAMEWORK_NAME).toUpperCase());

    metricsRPCPort = Integer.parseInt(System.getenv(Constants.METRICS_RPC_PORT));
    metricsIntervalMs = tonyConf.getInt(TonyConfigurationKeys.TASK_METRICS_UPDATE_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_METRICS_UPDATE_INTERVAL_MS);

    Utils.initYarnConf(yarnConf);
    Utils.initHdfsConf(hdfsConf);
  }

  private String registerAndGetClusterSpec() {
    ContainerId containerId = ContainerId.fromString(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()));
    String hostName = Utils.getCurrentHostName();
    LOG.info("ContainerId is: " + containerId + " HostName is: " + hostName);

    // Start the Heartbeater..
    scheduledThreadPool.scheduleAtFixedRate(new Heartbeater(),
        0, hbInterval, TimeUnit.MILLISECONDS);

    LOG.info("Connecting to " + amHost + ":" + amPort + " to register worker spec: " + jobName + " " + taskIndex + " "
             + hostName + ":" + this.rpcPort.getPort());
    return Utils.pollTillNonNull(() ->
        proxy.registerWorkerSpec(jobName + ":" + taskIndex,
            hostName + ":" + this.rpcPort.getPort()), 3, 0);
  }

  public void callbackInfoToAM(String taskId, String callbackInfo) throws IOException, YarnException {
    proxy.registerCallbackInfo(taskId, callbackInfo);
  }

  private void registerTensorBoardUrl() {
    String hostName = Utils.getCurrentHostName();
    String tbUrl = hostName + ":" + this.tbPort.getPort();
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

  public boolean isGangMode() {
    return distributedMode == TonyConfigurationKeys.DistributedMode.GANG;
  }

  public Map<String, String> getShellEnv() {
    return shellEnv;
  }

  public int getTimeOut() {
    return timeOut;
  }

  public String getJobName() {
    return jobName;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public TonyConfigurationKeys.DistributedMode getDistributedMode() {
    return distributedMode;
  }

  public String getTaskCommand() {
    return taskCommand;
  }

  public String getClusterSpec() {
    return clusterSpec;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public Configuration getTonyConf() {
    return tonyConf;
  }

  public int getTbPort() {
    return tbPort.getPort();
  }

  public boolean isChief() {
    return isChief;
  }

  @VisibleForTesting
  public void setTonyConf(Configuration tonyConf) {
    this.tonyConf = tonyConf;
  }

  @VisibleForTesting
  public void setChief(boolean chief) {
    isChief = chief;
  }

  @VisibleForTesting
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public void setTaskCommand(String taskCommand) {
    this.taskCommand = taskCommand;
  }
}
