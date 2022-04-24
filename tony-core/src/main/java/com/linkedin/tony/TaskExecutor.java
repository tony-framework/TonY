/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.impl.ApplicationRpcClient;
import com.linkedin.tony.util.Utils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * Content that we want to run in the containers. TaskExecutor will register itself with AM and fetch cluster spec from
 * AM. After the cluster spec is collected, TaskExecutor will set up local environment and start the worker task.
 */
public class TaskExecutor implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(TaskExecutor.class);
  private static final int GENERAL_EXIT_CODE = 1;
  private static final int DEFAULT_REQUEST_POLL_INTERVAL = 1;
  private static final int DEFAULT_REQUEST_POLL_TIMEOUT = 60;
  public static final String MARK_LOST_CONNECTION_ENV_KEY = "MARK_TASK_EXECUTOR_LOST_CONNECTION_WITH_AM";

  @VisibleForTesting
  protected Configuration tonyConf = new Configuration(false);

  private ServerPort rpcPort;
  private ServerPort tbPort;

  private int executionTimeOut;
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
  private int maxConsecutiveHBMiss;
  private int numFailedHBAttempts = 0;
  private String frameworkType;
  private String appIdString;
  private int registerToAMTimeout;

  private static Framework.TaskExecutorAdapter taskRuntimeAdapter;

  private volatile boolean markedAsLostConnectionWithAM = false;

  private String containerLogDir;
  private int executionErrorMsgOutputMaxDepth;

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
    if (taskRuntimeAdapter.needReserveTBPort()) {
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

    assert taskRuntimeAdapter == null;
    taskRuntimeAdapter = FrameworkRuntimeProvider.getTaskAdapter(executor.frameworkType, executor);

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
    try (TaskExecutor executor = requireNonNull(createExecutor())) {
      // If not reusing port, then reserve them up until before the underlying TF process is
      // launched. See <a href="https://github.com/linkedin/TonY/issues/365">this issue</a> for
      // details.
      if (!executor.isTFGrpcReusingPort()) {
        LOG.info("Releasing reserved RPC port before launching tensorflow process.");
        executor.releasePort(executor.rpcPort);
      }

      if (!executor.isTBServerReusingPort()) {
        LOG.info("Releasing reserved TB port before launching tensorflow process.");
        executor.releasePort(executor.tbPort);
      }

      CompletableFuture<Integer> childProcessFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return taskRuntimeAdapter.run();
        } catch (Exception e) {
          LOG.error("Errors on running child process.", e);
        }
        return GENERAL_EXIT_CODE;
      });

      int exitCode;
      while (true) {
        if (executor.markedAsLostConnectionWithAM) {
          exitCode = GENERAL_EXIT_CODE;
          break;
        }

        if (childProcessFuture.isDone()) {
          exitCode = childProcessFuture.getNow(GENERAL_EXIT_CODE);
          break;
        }
      }

      // START - worker skew testing:
      executor.skewAndHangIfTesting();
      // END - worker skew testing:
      executor.registerExecutionResult(exitCode, executor.jobName, String.valueOf(executor.taskIndex));

      LOG.info("Child process exited with exit code: " + exitCode);

      if (exitCode == 0) {
        System.exit(0);
      }

      String errorMsg = executor.getExecutionErrorLog();

      try {
        Utils.shutdownThreadPool(executor.scheduledThreadPool);
        if (!childProcessFuture.isDone()) {
          childProcessFuture.cancel(true);
        }
      } catch (Exception e) {
        System.exit(exitCode);
      }

      throw new Exception("Execution exit code: " + exitCode + ", error messages: \n" + errorMsg);
    }
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
    executionTimeOut = tonyConf.getInt(TonyConfigurationKeys.TASK_EXECUTION_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_TASK_EXECUTION_TIMEOUT);
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
    frameworkType = tonyConf.get(TonyConfigurationKeys.FRAMEWORK_NAME,
            TonyConfigurationKeys.DEFAULT_FRAMEWORK_NAME).toUpperCase();

    metricsRPCPort = Integer.parseInt(System.getenv(Constants.METRICS_RPC_PORT));
    metricsIntervalMs = tonyConf.getInt(TonyConfigurationKeys.TASK_METRICS_UPDATE_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_METRICS_UPDATE_INTERVAL_MS);

    maxConsecutiveHBMiss = tonyConf.getInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS,
            TonyConfigurationKeys.DEFAULT_TASK_MAX_MISSED_HEARTBEATS);

    // Only for test case.
    markedAsLostConnectionWithAM = shellEnv.getOrDefault(MARK_LOST_CONNECTION_ENV_KEY, "false")
            .equalsIgnoreCase("true");

    registerToAMTimeout = tonyConf.getInt(TonyConfigurationKeys.TASK_EXECUTOR_MAX_REGISTRY_SEC,
            TonyConfigurationKeys.DEFAULT_TASK_EXECUTOR_MAX_REGISTRY_SEC);

    containerLogDir = System.getProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR);
    executionErrorMsgOutputMaxDepth = tonyConf.getInt(TonyConfigurationKeys.TASK_EXECUTOR_EXECUTION_ERROR_MESSAGE_MAX_DEPTH,
            TonyConfigurationKeys.DEFAULT_TASK_EXECUTOR_EXECUTION_ERROR_MESSAGE_MAX_DEPTH);

    Utils.initYarnConf(yarnConf);
    Utils.initHdfsConf(hdfsConf);
  }

  private String registerAndGetClusterSpec() throws IOException {
    ContainerId containerId = ContainerId.fromString(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()));
    String hostName = Utils.getCurrentHostName();
    LOG.info("ContainerId is: " + containerId + " HostName is: " + hostName);

    // Start the Heartbeater..
    scheduledThreadPool.scheduleAtFixedRate(new Heartbeater(),
        0, hbInterval, TimeUnit.MILLISECONDS);

    LOG.info("Connecting to " + amHost + ":" + amPort + " to register worker spec: " + jobName + " " + taskIndex + " "
             + hostName + ":" + rpcPort.getPort());

    String taskId = String.format("%s:%s", jobName, taskIndex);
    String hostAndPort = String.format("%s:%s", hostName, rpcPort.getPort());

    String registerToAmResult =
            Utils.pollTillNonNull(() -> proxy.registerWorkerSpec(taskId, hostAndPort),
                DEFAULT_REQUEST_POLL_INTERVAL, registerToAMTimeout);
    if (registerToAmResult == null) {
      throw new IOException("Errors on registering to AM, maybe due to the network failure.");
    }

    return Utils.pollTillConditionReached(
            () -> proxy.getClusterSpec(taskId),
            x -> StringUtils.isNotEmpty(x),
            () -> null,
            DEFAULT_REQUEST_POLL_INTERVAL,
            0
    );
  }

  public void callbackInfoToAM(String taskId, String callbackInfo) throws IOException {
    String callbackResult = Utils.pollTillNonNull(() -> {
      proxy.registerCallbackInfo(taskId, callbackInfo);
      return StringUtils.EMPTY;
    }, DEFAULT_REQUEST_POLL_INTERVAL, DEFAULT_REQUEST_POLL_TIMEOUT);

    if (callbackResult == null) {
      throw new IOException("Errors on calling back task info to AM, task id: "
              + taskId + ", callback info: " + callbackInfo);
    }
  }

  private void registerTensorBoardUrl() {
    String hostName = Utils.getCurrentHostName();
    String tbUrl = hostName + ":" + this.tbPort.getPort();
    LOG.info("TensorBoard address : " + tbUrl);
    String response = Utils.pollTillNonNull(() -> proxy.registerTensorBoardUrl(tbUrl),
            DEFAULT_REQUEST_POLL_INTERVAL, DEFAULT_REQUEST_POLL_TIMEOUT);
    if (response != null) {
      LOG.info("Register TensorBoard response: " + response);
    }
  }

  private void registerExecutionResult(int exitCode, String jobName, String jobIndex) {
    String sessionId = System.getenv(Constants.SESSION_ID);
    String response = Utils.pollTillNonNull(
        () -> proxy.registerExecutionResult(exitCode, jobName, jobIndex, sessionId),
            DEFAULT_REQUEST_POLL_INTERVAL, DEFAULT_REQUEST_POLL_TIMEOUT);
    if (response != null) {
      LOG.info("AM response for result execution run: " + response);
    }
  }

  @Override
  public void close() throws Exception {
    if (isTFGrpcReusingPort()) {
      LOG.info("TensorFlow process exited, releasing reserved RPC port.");
      releasePort(rpcPort);
    }

    if (isTBServerReusingPort()) {
      LOG.info("Tensorflow process exited, releasing reserved TB port.");
      releasePort(tbPort);
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
        if (++numFailedHBAttempts > maxConsecutiveHBMiss) {
          LOG.error("[" + taskId + "] Exceeded max number of allowed failed heart beat send attempts. "
              + "Going to stop heartbeating!", e);
          markedAsLostConnectionWithAM =  true;
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

  public int getExecutionTimeout() {
    return executionTimeOut;
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

  public String getExecutionStdErrFile() {
    return String.format("%s%s%s", containerLogDir, File.separatorChar, Constants.TASK_EXECUTOR_EXECUTION_STDERR_FILENAME);
  }

  public String getExecutionStdOutFile() {
    return String.format("%s%s%s", containerLogDir, File.separatorChar, Constants.TASK_EXECUTOR_EXECUTION_STDOUT_FILENAME);
  }

  /** Only for test case. */
  @VisibleForTesting
  protected void setContainerLogDir(String containerLogDir) {
    this.containerLogDir = containerLogDir;
  }

  /** Only for test case. */
  @VisibleForTesting
  protected void setExecutionErrorMsgOutputMaxDepth(int executionErrorMsgOutputMaxDepth) {
    this.executionErrorMsgOutputMaxDepth = executionErrorMsgOutputMaxDepth;
  }

  @VisibleForTesting
  protected String getExecutionErrorLog() throws IOException {
    String executionErrorFile = getExecutionStdErrFile();
    File file = new File(executionErrorFile);
    if (!file.exists()) {
      return StringUtils.EMPTY;
    }

    try (FileInputStream inputStream = new FileInputStream(executionErrorFile);
        BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")))) {
      String line;
      int lineNumber = 0;
      StringBuilder errorMsgBuilder = new StringBuilder();
      while (true) {
        line = bufferedReader.readLine();
        if (line == null) {
          break;
        }
        if (lineNumber++ >= executionErrorMsgOutputMaxDepth) {
          break;
        }
        errorMsgBuilder.append(line);
        errorMsgBuilder.append("\n");
      }
      return errorMsgBuilder.toString();
    }
  }
}
