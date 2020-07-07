/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.impl.ApplicationRpcClient;
import com.linkedin.tony.util.Utils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;

import static com.linkedin.tony.TonyConfigurationKeys.MLFramework;
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

  private ServerSocket rpcSocket;

  private ServerSocket tbSocket;

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
  private Configuration yarnConf = new Configuration(false);
  private Configuration hdfsConf = new Configuration(false);
  private ApplicationRpcClient proxy;
  private Map<String, String> shellEnv = new HashMap<>();
  private int hbInterval;
  private final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(2);
  private int numFailedHBAttempts = 0;
  private MLFramework framework;
  public static final Range<Integer> PORT_RANGE = Range.between(1, 65535);
  public static final String PORT_FILE_PREFIX = "___port___";
  public static final java.nio.file.Path PORT_FILE_DIR = Paths.get(System.getProperty("java.io"
      + ".tmpdir"));


  protected TaskExecutor() { }

  /**
   * Creates a ServerSocket{@link java.net.ServerSocket} with specified port
   * @return the created ServerSocket{@link java.net.ServerSocket}, returns null
   *         if not successfully created.
   */
  @VisibleForTesting
  ServerSocket createServerSocket(int port) throws IOException {
    ServerSocket serverSocket;
    try {
      serverSocket = new ServerSocket(port);
    } catch (IOException e) {
      LOG.debug("Port " + port + " is not available");
      return null;
    } catch (Exception e) {
      LOG.error(e);
      return null;
    }

    try {
      Files.createFile(PORT_FILE_DIR.resolve(PORT_FILE_PREFIX + port));
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Port " + port + " is not available");
      serverSocket.close();
      return null;
    } catch (Exception e) {
      LOG.error(e);
      serverSocket.close();
      return null;
    }
    return serverSocket;
  }

  /**
   * Creates a ServerSocket{@link java.net.ServerSocket} with port within the defined range
   * {@link TaskExecutor#PORT_RANGE}
   * @return the created ServerSocket{@link java.net.ServerSocket},
   *         returns null if all ports within the range are iterated through but still fails to
   *         create a server socket.
   */
  private ServerSocket createServerSocket() throws IOException {
    // To prevent other process grabbing the reserved port between releasing the port{@link #releasePorts()}
    // and task command process {@link #taskCommand} starts, task executor reserves the port till
    // the task command finishes by following below approach -
    // Task executor will iterate over defined port range {@link #PORT_RANGE} and check a port's
    // availability based on the existence of corresponding port file. If socket can be created with
    // the port and corresponding port file doesn't exist, a temp file tracking that port will be
    // created under a shared directory {@link #PORT_FILE_DIR} and that port will be reserved for
    // the process{@link #taskCommand}. The file will be deleted upon the process finishes.
    ServerSocket serverSocket = null;
    for (int port = PORT_RANGE.getMinimum(); port <= PORT_RANGE.getMaximum(); port++) {
      serverSocket = createServerSocket(port);
      if (serverSocket != null) {
        LOG.info("Successfully created server socket with " + serverSocket.getLocalPort());
        return serverSocket;
      }
    }
    return serverSocket;
  }

  /**
   * We bind to available ports.
   */
  private void setupPorts() throws IOException {
    // Reserve a rpcSocket rpcPort.
    this.rpcSocket = requireNonNull(this.createServerSocket());
    LOG.info("Reserved rpcPort: " + this.rpcSocket.getLocalPort());

    // With Estimator API, there is a separate lone "chief" task that runs TensorBoard.
    // With the low-level distributed API, worker 0 runs TensorBoard.
    if (isChief) {
      this.tbSocket = requireNonNull(this.createServerSocket());
      this.registerTensorBoardUrl();
      this.shellEnv.put(Constants.TB_PORT, String.valueOf(this.tbSocket.getLocalPort()));
      LOG.info("Reserved tbPort: " + this.tbSocket.getLocalPort());
    }
  }

  /**
   * Release the reserved ports if any. This method has to be invoked before the task command
   * process {@link #taskCommand} starts so that the ports can be available to the process.
   * @throws IOException
   */
  private void releasePorts() throws IOException {
      try {
        if (this.rpcSocket != null) {
          this.rpcSocket.close();
        }
      } finally {
        if (this.tbSocket != null) {
          this.tbSocket.close();
        }
      }
  }

  /**
   * Delete port files if any. This method has to be invoked after task command
   * process {@link #taskCommand} finishes.
   * @throws IOException
   */
  private void deletePortFiles() throws IOException {
    if (this.rpcSocket != null) {
      Files.deleteIfExists(PORT_FILE_DIR.resolve(PORT_FILE_PREFIX + this.rpcSocket.getLocalPort()));
    }
    if (this.tbSocket != null) {
      Files.deleteIfExists(PORT_FILE_DIR.resolve(PORT_FILE_PREFIX + this.tbSocket.getLocalPort()));
    }
  }

  private static TaskExecutor createExecutor() throws Exception {
    TaskExecutor executor = new TaskExecutor();
    executor.initConfigs();
    Utils.extractResources();

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

    executor.setupPorts();
    executor.clusterSpec = executor.registerAndGetClusterSpec();

    if (executor.clusterSpec == null) {
      LOG.error("Failed to register worker with AM.");
      throw new Exception("Failed to register worker with AM.");
    }
    LOG.info("Successfully registered and got cluster spec: " + executor.clusterSpec);

    switch (executor.framework) {
      case TENSORFLOW:
        LOG.info("Setting up TensorFlow job...");
        executor.shellEnv.put(Constants.JOB_NAME, String.valueOf(executor.jobName));
        executor.shellEnv.put(Constants.TASK_INDEX, String.valueOf(executor.taskIndex));
        executor.shellEnv.put(Constants.CLUSTER_SPEC, String.valueOf(executor.clusterSpec));
        executor.shellEnv.put(Constants.TF_CONFIG, Utils.constructTFConfig(executor.clusterSpec, executor.jobName, executor.taskIndex));
        break;
      case PYTORCH:
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
      case MXNET:
        LOG.info("Setting up MXNet job...");
        String[] dmlcServer = Utils.parseClusterSpecForMXNet(executor.clusterSpec);
        if (dmlcServer == null) {
          System.exit(-1);
        }
        int numServer = executor.tonyConf.getInt(TonyConfigurationKeys.getInstancesKey(Constants.SERVER_JOB_NAME), 0);
        int numWorker = executor.tonyConf.getInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 0);
        LOG.info("init DMLC is: " + dmlcServer[0] + " port: " + dmlcServer[1]);
        LOG.info("init DMLC ROLE: " + executor.jobName);
        LOG.info("init DMLC NUM_PS: " + numServer);
        LOG.info("init DMLC NUM_WORKER: " + numWorker);
        executor.shellEnv.put(Constants.DMLC_ROLE, executor.jobName);
        executor.shellEnv.put(Constants.DMLC_PS_ROOT_URI, dmlcServer[0]);
        executor.shellEnv.put(Constants.DMLC_PS_ROOT_PORT, dmlcServer[1]);
        executor.shellEnv.put("DMLC_LOCAL", "0");
        //executor.shellEnv.put("DMLC_USE_KUBERNETES", "0");
        executor.shellEnv.put(Constants.DMLC_NUM_SERVER, String.valueOf(numServer));
        executor.shellEnv.put(Constants.DMLC_NUM_WORKER, String.valueOf(numWorker));
        //executor.shellEnv.put(Constants.PS_VERBOSE, "2");
        break;
      case HOROVOD:
        // No extra environment variables needed; horovodrun takes care of setup.
        // Setting TF_CONFIG causes problems if "chief" isn't set.
        break;
      default:
        throw new RuntimeException("Unsupported executor framework: " + executor.framework);
    }
    return executor;
  }

  public static void main(String[] unused) throws Exception {
    LOG.info("TaskExecutor is running..");
    TaskExecutor executor = null;
    try {
      executor = requireNonNull(createExecutor());
    } finally {
      if (executor != null) {
        // release ports so that subsequent taskCommand process can consume them.
        executor.releasePorts();
      }
    }

    int exitCode = -1;
    try {
      exitCode = Utils.executeShell(executor.taskCommand, executor.timeOut, executor.shellEnv);
    } finally {
      executor.deletePortFiles();
    }
    // START - worker skew testing:
    executor.skewAndHangIfTesting();
    // END - worker skew testing:
    executor.registerExecutionResult(exitCode, executor.jobName, String.valueOf(executor.taskIndex));

    LOG.info("Child process exited with exit code " + exitCode);
    System.exit(exitCode);
  }

  protected void initConfigs() {
    jobName = System.getenv(Constants.JOB_NAME);
    taskIndex = Integer.parseInt(System.getenv(Constants.TASK_INDEX));
    numTasks = Integer.parseInt(System.getenv(Constants.TASK_NUM));
    taskId = jobName + ":" + taskIndex;
    LOG.info("Executor is running task " + taskId);

    String isChiefEnvValue = System.getenv(Constants.IS_CHIEF);
    isChief = Boolean.parseBoolean(isChiefEnvValue);

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
    framework = MLFramework.valueOf(
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
             + hostName + ":" + this.rpcSocket.getLocalPort());
    return Utils.pollTillNonNull(() ->
        proxy.registerWorkerSpec(jobName + ":" + taskIndex,
            hostName + ":" + this.rpcSocket.getLocalPort()), 3, 0);
  }

  private void registerTensorBoardUrl() {
    String hostName = Utils.getCurrentHostName();
    String tbUrl = hostName + ":" + this.tbSocket.getLocalPort();
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
}
