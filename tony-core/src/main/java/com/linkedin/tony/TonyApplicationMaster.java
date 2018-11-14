/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.linkedin.tony.rpc.ApplicationRpc;
import com.linkedin.tony.rpc.ApplicationRpcServer;
import com.linkedin.tony.rpc.TaskUrl;
import com.linkedin.tony.tensorflow.TensorFlowContainerRequest;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.tensorflow.TonySession.TonyTask;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import py4j.GatewayServer;

import static com.linkedin.tony.Constants.*;
import static com.linkedin.tony.TonyConfigurationKeys.*;
import static com.linkedin.tony.Utils.*;


public class TonyApplicationMaster {
  private static final Log LOG = LogFactory.getLog(TonyApplicationMaster.class);

  /**
   * Metadata of jobs
   */
  private ApplicationAttemptId appAttemptID = null;
  private static String appIdString;
  private String amHostPort;
  private static FileSystem fs;
  private static String tonyHistoryFolder;
  private static Path jobDir;

  public static class Metadata {
    public String id;
    public String url;
    public long started;
    public long completed;
    public String status;
    public String user;
    public Metadata(String id, String url, long started, long completed, String status, String user) {
      this.id = id;
      this.url = url;
      this.started = started;
      this.completed = completed;
      this.status = status;
      this.user = user;
    }
  }

  // Container info
  private int amRetryCount;
  private long workerTimeout;
  private String hdfsClasspath;
  private String baseTaskCommand;
  private String pythonVenvZip;
  private int amPort;
  private ByteBuffer allTokens;
  private Map<String, LocalResource> localResources = new ConcurrentHashMap<>();
  private Configuration tonyConf = new Configuration();
  private ContainerId containerId;

  /** The environment set up for the TaskExecutor **/
  private Map<String, String> containerEnv = new ConcurrentHashMap<>();

  /** The environment passed from users to the training job. Note this is very different from the above. **/
  private Map<String, String> shellEnv = new HashMap<>();
  private Map<String, List<Container>> sessionContainersMap = new ConcurrentHashMap<>();

  /** Node manager delegates **/
  private NMCallbackHandler containerListener;
  private NMClientAsync nmClientAsync;
   /** Resource manager **/
  private AMRMClientAsync<ContainerRequest> amRMClient;
   /** Job progress **/
  private AtomicInteger numCompletedWorkerTasks = new AtomicInteger();
  private long numTotalWorkerTasks = 1;

  private AtomicInteger numRequestedContainers = new AtomicInteger();
  private Map<String, List<ContainerRequest>> jobTypeToContainerRequestsMap = new HashMap<>();

  // allocationRequestIds are allocated to tasks sequentially. lastAllocationRequestId
  // tracks the latest allocationRequestId allocated.
  private long lastAllocationRequestId = 0;

  /** Tony session **/
  private TonySession session = new TonySession(); // Create a dummy session for single node training.
  private TonySession.Builder sessionBuilder;

  /** Configuration **/
  private static Configuration yarnConf;
  private Configuration hdfsConf;

  /** Cluster spec **/
  private ApplicationRpcServer rpcServer;

  /** Set to false when testing locally / running in insecure cluster **/
  private boolean secureMode;

  /** Single node training **/
  private boolean singleNode;
  private boolean preprocessFinished = false;
  private int preprocessExitCode = 0;
  private String proxyUrl;

  /** Preprocessing job **/
  private boolean enablePreprocessing = false;

  /** Lifecycle control **/
  private long appTimeout;

  private boolean clientSignalToStop = false; // client signal to stop

  /** HeartBeat monitor **/
  private final AbstractLivelinessMonitor<TonyTask> hbMonitor;
  private int hbInterval;
  private int maxConsecutiveHBMiss;
  private volatile boolean taskHasMissesHB = false;
  private Thread mainThread;

  /** Handle different machine frameworks **/
  private MLFramework framework;

  private TonyApplicationMaster() {
    hdfsConf = new Configuration();
    yarnConf = new Configuration();

    hbMonitor = new AbstractLivelinessMonitor<TonyTask>("Tony Task liveliness Monitor") {
      @Override
      protected void expire(TonyTask task) {
        onTaskDeemedDead(task);
      }

      @Override
      protected void serviceStart() throws Exception {
        setMonitorInterval(hbInterval * 3);
        setExpireInterval(hbInterval * Math.max(3, maxConsecutiveHBMiss)); // Be at least == monitoring interval
        super.serviceStart();
      }
    };
  }

  /**
   * Parse command line options and initialize TonyApplicationMaster
   * @return whether the initialization is successful or not.
   */
  private boolean init(String[] args) {
    try {
      Utils.unzipArchive(Constants.TONY_ZIP_NAME, "./");
    } catch (IOException e) {
      LOG.error("Failed to unzip: " + Constants.TONY_ZIP_NAME, e);
      return false;
    }
    tonyConf.addResource(new Path(Constants.TONY_FINAL_XML));
    if (System.getenv(HDFS_SITE_CONF) != null) {
      hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      yarnConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    }
    if (System.getenv(Constants.HDFS_CONF_PATH) != null) {
      hdfsConf.addResource(new Path(System.getenv(Constants.HDFS_CONF_PATH)));
      containerEnv.put(Constants.HDFS_CONF_PATH, System.getenv(Constants.HDFS_CONF_PATH));
    }
    if (System.getenv(Constants.YARN_CONF_PATH) != null) {
      yarnConf.addResource(new Path(System.getenv(Constants.YARN_CONF_PATH)));
      containerEnv.put(Constants.YARN_CONF_PATH, System.getenv(Constants.YARN_CONF_PATH));
    }

    try {
      fs = FileSystem.get(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to create FileSystem object", e);
      return false;
    }

    hbMonitor.init(tonyConf);

    Options opts = Utils.getCommonOptions();
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Got exception while parsing options", e);
      return false;
    }
    Map<String, String> envs = System.getenv();
    String[] shellEnvs = cliParser.getOptionValues("shell_env");
    shellEnv = Utils.parseKeyValue(shellEnvs);
    String[] containerEnvs = cliParser.getOptionValues("container_env");
    containerEnv.putAll(Utils.parseKeyValue(containerEnvs));
    pythonVenvZip = cliParser.getOptionValue("python_venv");

    baseTaskCommand = buildBaseTaskCommand(
        pythonVenvZip,
        cliParser.getOptionValue("python_binary_path"),
        cliParser.getOptionValue("executes"),
        cliParser.getOptionValue("task_params"));

    appTimeout = tonyConf.getInt(TonyConfigurationKeys.APPLICATION_TIMEOUT,
                                 TonyConfigurationKeys.DEFAULT_APPLICATION_TIMEOUT);
    workerTimeout = tonyConf.getInt(TonyConfigurationKeys.WORKER_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_WORKER_TIMEOUT);
    hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");
    amRetryCount = tonyConf.getInt(TonyConfigurationKeys.AM_RETRY_COUNT,
        TonyConfigurationKeys.DEFAULT_AM_RETRY_COUNT);
    singleNode = tonyConf.getBoolean(TonyConfigurationKeys.IS_SINGLE_NODE,
        TonyConfigurationKeys.DEFAULT_IS_SINGLE_NODE);
    secureMode = tonyConf.getBoolean(TonyConfigurationKeys.SECURITY_ENABLED,
        TonyConfigurationKeys.DEFAULT_SECURITY_ENABLED);
    enablePreprocessing = tonyConf.getBoolean(TonyConfigurationKeys.ENABLE_PREPROCESSING_JOB,
                                              TonyConfigurationKeys.DEFAULT_ENABLE_PREPROCESSING_JOB);
    containerId = ContainerId.fromString(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    appIdString = containerId.getApplicationAttemptId().getApplicationId().toString();
    hbInterval = tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS);
    maxConsecutiveHBMiss = tonyConf.getInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS,
        TonyConfigurationKeys.DEFAULT_TASK_MAX_MISSED_HEARTBEATS);
    framework = MLFramework.valueOf(tonyConf.get(TonyConfigurationKeys.FRAMEWORK_NAME,
                                                 TonyConfigurationKeys.DEFAULT_FRAMEWORK_NAME).toUpperCase());
    tonyHistoryFolder = tonyConf.get(TonyConfigurationKeys.TONY_HISTORY_LOCATION,
                                     TonyConfigurationKeys.DEFAULT_TONY_HISTORY_LOCATION);
    return true;
  }

  @VisibleForTesting
  static String buildBaseTaskCommand(String pythonVenvZip, String pythonBinaryPath, String script,
      String taskParams) {
    String pythonInterpreter = "";
    if (pythonVenvZip == null || pythonBinaryPath.startsWith("/")) {
      if (pythonBinaryPath != null) {
        pythonInterpreter = pythonBinaryPath;
      }
    } else {
      // Note that we always extract the Python venv zip to a "venv" (Constants.PYTHON_VENV_DIR) directory.
      pythonInterpreter = Constants.PYTHON_VENV_DIR + File.separatorChar + pythonBinaryPath;
    }

    String baseTaskCommand = pythonInterpreter + " " + script;

    if (taskParams != null) {
      baseTaskCommand += " " + taskParams;
    }

    return baseTaskCommand;
  }

  private void buildTensorFlowSession() {
    String taskCommand = "'" + baseTaskCommand + "'";
    LOG.info("Final task command: " + taskCommand);

    TonySession.Builder builder = new TonySession.Builder()
        .setTaskCmd(taskCommand)
        .setVenv(pythonVenvZip)
        .setAMAddress(amHostPort)
        .setShellEnv(shellEnv)
        .setTonyConf(tonyConf)
        .setTaskExecutorJVMArgs(tonyConf.get(TonyConfigurationKeys.TASK_EXECUTOR_JVM_OPTS,
            TonyConfigurationKeys.DEFAULT_TASK_EXECUTOR_JVM_OPTS));
    sessionBuilder = builder;
    session = builder.build();
  }

  /**
   * Entry point of TonyApplicationMaster
   * The workflow of a training job in AM
   * prepare -> start -> failed    -> reset -> retry if amRetryCount > 0 otherwise fail the job.
   *                  -> succeeded -> stop -> job succeeded
   * @param args the args from user inputs
   */
  public static void main(String[] args) {
    boolean succeeded = false;
    long started;
    long completed;
    TonyApplicationMaster am = new TonyApplicationMaster();
    boolean sanityCheck = am.init(args);
    if (!sanityCheck) {
      System.exit(-1);
    }
    if (!am.prepare()) {
      System.exit(-1);
    }
    am.mainThread = Thread.currentThread();
    boolean exitOnError = false;
    started = System.currentTimeMillis();
    do {
      // Crash AM on purpose during AM crash tests.
      String shouldCrash = System.getenv(Constants.TEST_AM_CRASH);
      if (shouldCrash != null && shouldCrash.equals("true")) {
        exitOnError = true;
        break;
      }

      try {
        am.start();
      } catch (Exception e) {
        LOG.error("Exception when we're starting TonyAM", e);
        System.exit(-1);
      }
      succeeded = am.monitor();
      if (succeeded || am.amRetryCount == 0) {
        LOG.info("Result: " + succeeded + ", retry count: " + am.amRetryCount);
        break;
      }
      // Prepare for retryCount.
      am.reset();
      LOG.info("Retrying, remaining retry count" + am.amRetryCount);
      am.amRetryCount -= 1;
    } while (!am.singleNode); // We don't retry on single node training.
    // Wait for the worker nodes to finish (The interval between registering the exit code to final exit)
    am.stop();
    completed = System.currentTimeMillis();
    am.printTaskUrls();
    if (exitOnError) {
      LOG.fatal("Error running TonyApplicationMaster !!");
      System.exit(-1);
    }

    // By this time jobDir should have been set
    createHistoryFile(fs, createMetadataObj(started, completed, succeeded), jobDir);

    if (succeeded) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(-1);
    }
  }

  private static Metadata createMetadataObj(long started, long completed, boolean status) {
    String jobStatus = status ? "SUCCEEDED" : "FAILED";
    String url = "http://" + yarnConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS) + "/cluster/app/" + appIdString;
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.error("User not found. Set user to null", e);
      user = null;
    }
    return new Metadata(appIdString, url, started, completed, jobStatus, user);
  }


  private static void createHistoryFile(FileSystem fs, Metadata metaObj, Path jobDir) {
    Path historyFile = new Path(jobDir, generateFileName(metaObj));
    try {
      fs.create(historyFile);
    } catch (IOException e) {
      LOG.error("Failed to create history file", e);
    }
  }

  /**
   * Prepare the application master. This part is shared across different retries.
   */
  private boolean prepare() {
    LOG.info("Preparing application master..");

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(yarnConf);
    nmClientAsync.start();

    String hostname = Utils.getCurrentHostName();
    rpcServer = setupRPCService(hostname);

    // Init AMRMClient
    AMRMClientAsync.AbstractCallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(yarnConf);
    amRMClient.start();

    RegisterApplicationMasterResponse response;
    try {
      response = amRMClient.registerApplicationMaster(hostname, amPort, null);
      amHostPort = hostname + ":" + amPort;
      LOG.info("RPC server running at: " + amHostPort);
      if (secureMode) {
        ClientToAMTokenIdentifier identifier =
            new ClientToAMTokenIdentifier(appAttemptID, UserGroupInformation.getCurrentUser().getShortUserName());
        byte[] secret = response.getClientToAMTokenMasterKey().array();
        ClientToAMTokenSecretManager secretManager = new ClientToAMTokenSecretManager(appAttemptID, secret);
        Token<? extends TokenIdentifier> token = new Token<>(identifier, secretManager);
        token.setService(new Text(amHostPort));
        rpcServer.setSecretManager(secretManager);
        UserGroupInformation.getCurrentUser().addToken(token);
        setupContainerCredentials();
      }
    } catch (Exception e) {
      LOG.error("Exception while preparing AM", e);
      return false;
    }
    if (!isJobDirSetup(fs, tonyHistoryFolder, appIdString) || !configFileExists(fs, jobDir)) {
      return false;
    }
    rpcServer.start();
    hbMonitor.start();
    return true;
  }

  /**
   * Create job directory under history folder.
   * Side effects: {@code jobDir} global variable will be set if function finished successfully.
   * Otherwise, it will just be an invalid Path object.
   * @param fs FileSystem object.
   * @param histFolder History folder location string.
   * @param appId Application ID string.
   * @return true if the job directory is successfully set up. False otherwise.
   */
  public boolean isJobDirSetup(FileSystem fs, String histFolder, String appId) {
    jobDir = new Path(histFolder + "/" + appId + "/");
    try {
      if (!fs.exists(jobDir)) {
        fs.mkdirs(jobDir);
        fs.setPermission(jobDir, new FsPermission((short) 0770));
      }
    } catch (IOException e) {
      LOG.error("Failed to create " + jobDir.toString(), e);
      return false;
    }
    return true;
  }

  /**
   * Generate config file in {@code jobDir} folder.
   * @param fs FileSystem object.
   * @param jobDir Path object of job directory (store all the files related to the job).
   * @return true if config file is successfully written to {@code jobDir}. False otherwise.
   */
  private boolean configFileExists(FileSystem fs, Path jobDir) {
    Path configFile = new Path(jobDir,"config.xml");
    try (FSDataOutputStream out = fs.create(configFile)) {
      tonyConf.writeXml(out);
    } catch (IOException e) {
      LOG.error("Failed to write config to XML", e);
      return false;
    }
    return true;
  }

  /**
   * This method start the training job. It also does the training preprocessing in this function as well
   * preprocessing job is used to abstract out common computation in each worker to a single place, however,
   * we do plan to move the preprocessing job to a worker node in the future to reduce the complexity of AM.
   * @throws IOException exception during HDFS file operations.
   * @throws InterruptedException during Thread.sleep.
   */
  private void start() throws Exception {

    int exitCode = 0;

    // Perform the preprocess job.
    if (enablePreprocessing || singleNode) {
       exitCode = doPreprocessingJob();
    }

    // Early exit for single node training.
    if (singleNode) {
      if (exitCode != 0) {
        LOG.info("Single node job exits with " + exitCode + ", exiting.");
        session.setFinalStatus(FinalApplicationStatus.FAILED, "Single node training failed..");
      } else {
        LOG.info("Single node job exits with " + exitCode + ", exiting.");
        session.setFinalStatus(FinalApplicationStatus.SUCCEEDED, "Single node job succeeded.");
      }
      return;
    }

    if (exitCode != 0) {
      return;
    }

    buildTensorFlowSession();
    scheduleTasks();
  }

  private void scheduleTasks() {
    session.setResources(yarnConf, hdfsConf, localResources, containerEnv, hdfsClasspath);
    List<TensorFlowContainerRequest> requests = session.getContainersRequests();
    numTotalWorkerTasks = requests.stream().filter(request -> request.getJobName().equals("worker")).count();
    for (TensorFlowContainerRequest request : requests) {
      scheduleTask(request);
    }
    numRequestedContainers.set(requests.size());
  }

  private void scheduleTask(TensorFlowContainerRequest request) {
    AMRMClient.ContainerRequest containerAsk = setupContainerRequestForRM(request);
    if (!jobTypeToContainerRequestsMap.containsKey(request.getJobName())) {
      jobTypeToContainerRequestsMap.put(request.getJobName(), new ArrayList<>());
    }
    jobTypeToContainerRequestsMap.get(request.getJobName()).add(containerAsk);
    amRMClient.addContainerRequest(containerAsk);
  }

  // Reset state to prepare for retryCount.
  private void reset() {
    List<Container> containers = sessionContainersMap.get(String.valueOf(session.sessionId));
    for (Container container : containers) {
      nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
      LOG.info("Stop a task in container: containerId = " + container.getId() + ", containerNode = "
               + container.getNodeId().getHost());
    }

    // Reset session and counters.
    session = sessionBuilder.build();

    numCompletedWorkerTasks.set(0);
    numRequestedContainers.set(0);
    rpcServer.reset();
    session.sessionId += 1;
  }

  /**
   * Monitor the TensorFlow training job.
   * @return if the tensorflow job finishes successfully.
   */
  private boolean monitor() {
    int attempt = 0;
    containerEnv.put(Constants.ATTEMPT_NUMBER, String.valueOf(attempt));
    long expireTime = appTimeout == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + appTimeout;
    while (true) {
      // Checking timeout
      if (System.currentTimeMillis() > expireTime) {
        LOG.error("Application times out.");
        return false;
      }

      // Check if client signals we should exit.
      if (clientSignalToStop) {
        LOG.info("Client signals AM to exit.");
        return true;
      }

      if (session.isTrainingFinished()) {
        LOG.info("Training has finished.");
        return true;
      }

      if (preprocessExitCode != 0) {
        LOG.info("Preprocess failed with exit code: " + preprocessExitCode);
        return false;
      }

      if (singleNode && preprocessFinished) {
        LOG.info("Single node training finished with exit code: " + preprocessExitCode);
        return preprocessExitCode == 0;
      }

      if (numCompletedWorkerTasks.get() == numTotalWorkerTasks) {
        LOG.info("Completed jobs: " + numCompletedWorkerTasks.get() + " total jobs: " + numTotalWorkerTasks);
        break;
      }
      LOG.info("Completed worker tasks: " + numCompletedWorkerTasks.get()
               + ", total worker tasks: " + numTotalWorkerTasks);

      if (this.taskHasMissesHB) {
        break;
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted", e);
      }
    }
    if (this.taskHasMissesHB) {
      session.setFinalStatus(FinalApplicationStatus.FAILED,
          "Application failed due to missed heartbeats");
    } else {
      session.updateSessionStatus();
    }

    LOG.info("Total completed worker tasks: " + numCompletedWorkerTasks.get()
        + ", total worker tasks: " + numTotalWorkerTasks);
    boolean success = true;
    FinalApplicationStatus status = session.getFinalStatus();
    String appMessage = session.getFinalMessage();
    if (status != FinalApplicationStatus.SUCCEEDED) {
      LOG.info("Tony session failed: " + appMessage);
      success = false;
    }
    return success;
  }

  /**
   * Returns the tasks whose containers have launched but not called {@link ApplicationRpc#registerWorkerSpec} yet.
   */
  private Set<TonyTask> getUnregisteredTasks() {
    return session.getTonyTasks().values().stream().flatMap(Arrays::stream)
        .filter(task -> task != null && task.getHost() == null)
        .collect(Collectors.toSet());
  }

  private void stop() {
    FinalApplicationStatus status = session.getFinalStatus();
    String appMessage = session.getFinalMessage();
    try {
      amRMClient.unregisterApplicationMaster(status, appMessage, null);
    } catch (Exception ex) {
      LOG.error("Failed to unregister application", ex);
    }
    nmClientAsync.stop();
    amRMClient.waitForServiceToStop(5000);
    amRMClient.stop();
    // Poll until TonyClient signals we should exit
    boolean result = Utils.poll(() -> clientSignalToStop, 1, 30);
    if (!result) {
      LOG.warn("TonyClient didn't signal Tony AM to stop.");
    }
  }

  // Run the preprocessing job and set up the common env variables for worker jobs.
  private int doPreprocessingJob() throws Exception {
    ServerSocket gatewayServerSocket = new ServerSocket(0);
    int gatewayServerPort = gatewayServerSocket.getLocalPort();
    // Set up py4j
    GatewayServer pyServer = new GatewayServer(this, gatewayServerPort);
    gatewayServerSocket.close();
    pyServer.start();

    HashMap<String, String> extraEnv = new HashMap<>(shellEnv);
    if (singleNode) {
      ServerSocket tbSocket = new ServerSocket(0);
      int tbPort = tbSocket.getLocalPort();
      extraEnv.put(Constants.TB_PORT, String.valueOf(tbPort));
      String tbUrl = Utils.getCurrentHostName() + ":" + tbPort;
      proxyUrl = tbUrl;
      LOG.info("Registering tensorboard url for single node training: " + tbUrl);
      registerTensorBoardUrlToRM(tbUrl);
      tbSocket.close();
    }
    LOG.info("Start python preprocessing");
    if (pythonVenvZip != null) {
      LOG.info("Unpacking python venv: " + pythonVenvZip);
      Utils.unzipArchive(pythonVenvZip, Constants.PYTHON_VENV_DIR);
    } else {
      LOG.warn("No Python virtual environment uploaded, using python_binary_path directly.");
    }

    extraEnv.put(Constants.PREPROCESSING_JOB, "true");

    /*
     * YARN sets $HOME to /user/yarn which users don't have access to write there.
     * Unfortunately, some services like Jupyter Notebook wants to write stuff there,
     * set it to user.dir (root of this container's address).
     */
    extraEnv.put("HOME", System.getProperty("user.dir"));
    extraEnv.put(Constants.PY4JGATEWAY, String.valueOf(gatewayServerPort));
    String taskCommand = baseTaskCommand;
    LOG.info("Executing command: " + taskCommand);
    int exitCode = Utils.executeShell(taskCommand, workerTimeout, extraEnv);

    preprocessExitCode = exitCode;
    preprocessFinished = true;

    // Short circuit if preprocessing job fails.
    if (exitCode != 0) {
      LOG.error("Preprocess job exits with " + exitCode + ", exiting.");
      session.setFinalStatus(FinalApplicationStatus.FAILED, "Preprocessing job failed.");
      return exitCode;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(
        System.getProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR)
        + File.separatorChar + Constants.AM_STDOUT_FILENAME))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.contains("Model parameters: ")) {
          String params = line.substring(line.indexOf("Model parameters: ") + "Model parameters: ".length());
          // Add serialized params to env
          containerEnv.put(Constants.TASK_PARAM_KEY, params);
          break;
        }
      }
    }
    return exitCode;
  }

  private void printTaskUrls() {
    if (session != null) {
      session.getTonyTasks()
          .values()
          .stream()
          .flatMap(Arrays::stream)
          .forEach(task -> Utils.printTaskUrl(task.getTaskUrl(), LOG));
    }
  }

  private ApplicationRpcServer setupRPCService(String hostname) {
    ApplicationRpcServer rpcServer = new ApplicationRpcServer(hostname, new RpcForClient(), yarnConf);
    amPort = rpcServer.getRpcPort();
    return rpcServer;
  }

  private final class RpcForClient implements ApplicationRpc {
    private static final long REGISTRATION_STATUS_INTERVAL_MS = 15 * 1000;

    private Set<String> registeredTasks = new HashSet<>();
    private long lastRegisterWorkerTime = System.currentTimeMillis();

    @Override
    public void reset() {
      registeredTasks =  new HashSet<>();
    }

    @Override
    public Set<TaskUrl> getTaskUrls() {
      LOG.info("Client requesting TaskUrls!");

      // Special handling for NotebookSubmitter.
      if (singleNode && proxyUrl != null) {
        HashSet<TaskUrl> additionalTasks = new HashSet<>();
        additionalTasks.add(new TaskUrl(Constants.DRIVER_JOB_NAME, "0", Utils.constructContainerUrl(
                          Utils.getCurrentHostName() + ":"
                          + System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name()), containerId)));
        additionalTasks.add(new TaskUrl(Constants.NOTEBOOK_JOB_NAME, "0", proxyUrl));
        return additionalTasks;
      }

      if (!singleNode && session != null && session.allTasksScheduled()) {
        return session.getTonyTasks().values().stream()
            .flatMap(tasks -> Arrays.stream(tasks).map(TonyTask::getTaskUrl))
            .collect(Collectors.toSet());
      }

      return Collections.emptySet();
    }

    @Override
    public String getClusterSpec() throws IOException {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsString(session.getClusterSpec());
    }

    @Override
    public void taskExecutorHeartbeat(String taskId) {
      TonyTask task = session.getTask(taskId);
      if (task != null) {
        LOG.debug("[" + taskId + "] Received HB Ping !!");
        hbMonitor.receivedPing(task);
      } else {
        LOG.warn("[" + taskId + "] Not registered for heartbeat monitoring !!");
      }
    }

    @Override
    public String registerWorkerSpec(String taskId, String spec) throws IOException {
      TonyTask task = session.getTask(taskId);
      if (task.getHost() == null) {
        LOG.info("Received cluster spec registration request from task " + taskId + " with spec: " + spec);
        task.setHostPort(spec);
        registeredTasks.add(taskId);

        // Use chief worker as coordinator.
        if (taskId.equals(COORDINATOR_ID) && framework == MLFramework.PYTORCH) {
          // Hard coded to use tcp:// as backend. TODO: support other backend as well later.
          shellEnv.put(Constants.INIT_METHOD, COMMUNICATION_BACKEND + spec);
        }

        // HB Registration should happen only after worker registration..
        // The Task registration timeout will take care of rescheduling the task
        // on another node..
        LOG.info("[" + taskId + "] Received Registration for HB !!");
        hbMonitor.register(task);
        killChiefWorkerIfTesting(taskId);
      }

      // Return null until all tasks have registered
      if (registeredTasks.size() == numRequestedContainers.get()) {
        LOG.info("All " + numRequestedContainers.get() + " tasks registered.");
        return getClusterSpec();
      } else {
        // Periodically print a list of all tasks we are still awaiting registration from.
        if (System.currentTimeMillis() - lastRegisterWorkerTime > REGISTRATION_STATUS_INTERVAL_MS) {
          Set<TonyTask> unregisteredTasks = getUnregisteredTasks();
          LOG.info(String.format("Received registrations from %d tasks, awaiting registration from %d tasks.",
              registeredTasks.size(), numRequestedContainers.get() - registeredTasks.size()));
          unregisteredTasks.forEach(t -> LOG.info(
                  String.format("Awaiting registration from task %s %s in %s on host %s",
                      t.getJobName(), t.getTaskIndex(),
                      (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
                      (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none")))
          );
          lastRegisterWorkerTime = System.currentTimeMillis();
        }
        return null;
      }
    }

    /**
     * This method was used to workaround an issue that the Python script finished while the container failed to
     * close due to GPU allocation issue, which doesn't exist anymore.
     *
     * Discussion: A benefit of decoupling registering execution result from container exit status is that we can decouple
     * tony from a specific resource manager's callback logic.
     * However, this easily introduces lots of bugs since we'd have 3 places to handle failures - register call, container
     * complete callback and heartbeat.
     * To make things easier, we decide to go back and piggyback on container completion to dictate the execution result
     * of a task.
     * Still keep this RPC call here for now.
     */
    @Override
    public String registerExecutionResult(int exitCode, String jobName, String jobIndex, String sessionId) {
      LOG.info("Received result registration request with exit code " + exitCode + " from " + jobName + " " + jobIndex);
      return "RECEIVED";
    }

    @Override
    public String registerTensorBoardUrl(String spec) throws Exception {
      LOG.info("Got request to update TensorBoard URL: " + spec);
      return registerTensorBoardUrlToRM(spec);
    }

    @Override
    public void finishApplication() {
      LOG.info("Client signals AM to finish application.");
      clientSignalToStop = true;
    }
  }

  private String registerTensorBoardUrlToRM(String spec) throws Exception {
    if (spec != null && appIdString != null) {
      try {
        // Post YARN-7974 or Hadoop 3.1.2 release
        // amRMClient.updateTrackingUrl(spec);
        @SuppressWarnings("JavaReflectionMemberAccess")
        Method method = AMRMClientAsync.class.getMethod("updateTrackingUrl", String.class);
        method.invoke(amRMClient, spec);
      } catch (NoSuchMethodException nsme) {
        LOG.warn("This Hadoop version doesn't have the YARN-7974 patch, TonY won't register TensorBoard URL with"
                 + "application's tracking URL");
      }
      return "SUCCEEDED";
    } else {
      return "FAILED";
    }
  }

  // Set up credentials for the containers.
  private void setupContainerCredentials() throws IOException {
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    String submitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation submitterUgi = UserGroupInformation.createRemoteUser(submitterUserName);
    submitterUgi.addCredentials(credentials);
  }

  private AMRMClient.ContainerRequest setupContainerRequestForRM(TensorFlowContainerRequest request) {
    Priority priority = Priority.newInstance(request.getPriority());
    Resource capability = Resource.newInstance(request.getMemory(), request.getVCores());
    Utils.setCapabilityGPU(capability, request.getGPU());
    session.addAllocationId(request.getJobName(), lastAllocationRequestId);
    AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, null, null, priority,
        lastAllocationRequestId++);
    LOG.info("Requested container ask: " + containerRequest.toString());
    return containerRequest;
  }

  private NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler();
  }

  /**
   * Node manager call back handler
   */
  static class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOG.info("Succeeded to stop container " + containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.info("Container Status: id =" + containerId + ", status =" + containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      LOG.info("Successfully started container " + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start container " + containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop container " + containerId);
    }

    @Override
    public void onContainerResourceIncreased(ContainerId containerId, Resource resource) { }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId, Resource resource) { }

    @Override
    public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) { }

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) { }

  }

  private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Completed containers: " + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        int exitStatus = containerStatus.getExitStatus();
        LOG.info("ContainerID = " + containerStatus.getContainerId()
            + ", state = " + containerStatus.getState()
            + ", exitStatus = " + exitStatus);
        String diagnostics = containerStatus.getDiagnostics();
        if (ContainerExitStatus.SUCCESS != exitStatus) {
          LOG.error(diagnostics);
        } else {
          LOG.info(diagnostics);
        }
        TonyTask task = session.getTask(containerStatus.getContainerId());

        if (task != null) {
          // Ignore tasks from past sessions.
          if (task.getSessionId() != session.sessionId) {
            return;
          }

          // Update TonySession on the state of the task.
          session.onTaskCompleted(task.getJobName(), task.getTaskIndex(), exitStatus);
          if (task.getJobName().equals(WORKER_JOB_NAME)) {
            numCompletedWorkerTasks.incrementAndGet();
          }

          // Unregister task after completion..
          // Since in the case of asynchronous exec, containers might
          // end at different times..
          LOG.info("Unregister task [" + task.getId() + "] from Heartbeat monitor..");
          hbMonitor.unregister(task);
        } else {
          LOG.warn("No task found for container : [" + containerStatus.getContainerId() + "]!");
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Allocated: " + containers.size() + " containers.");
      for (Container container : containers) {
        LOG.info("Launching a task in container"
            + ", containerId = " + container.getId()
            + ", containerNode = " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
            + ", resourceRequest = " + container.getResource());
        new ContainerLauncher(container, containerListener).run();
      }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {
    }

    @Override
    public void onShutdownRequest() { }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
    }

    @Override
    public float getProgress() {
      return (float) numCompletedWorkerTasks.get() / numTotalWorkerTasks;
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.info("Error " + throwable);
      amRMClient.stop();
    }
  }

  /**
   * The command to prepare inside containers.
   */
  private class ContainerLauncher implements Runnable {
    Container container;
    NMCallbackHandler containerListener;

    ContainerLauncher(Container container, NMCallbackHandler containerListener) {
      this.container = container;
      this.containerListener = containerListener;
    }

    /**
     * Set up container's launch command and start the container.
     */
    public void run() {
      // Specify session id in the env to distinguish between different sessions.
      containerEnv.put(Constants.SESSION_ID, String.valueOf(session.sessionId));
      Map<String, String> containerShellEnv = new ConcurrentHashMap<>(containerEnv);

      TonyTask task = session.getAndInitMatchingTask(container.getAllocationRequestId());

      Preconditions.checkNotNull(task, "Task was null! Nothing to schedule.");

      Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
      String[] resources = tonyConf.getStrings(TonyConfigurationKeys.getResourcesKey(task.getJobName()));
      if (null != resources) {
        for (String dir : resources) {
          Utils.addResource(dir, containerResources, hdfsConf);
        }
      }
      task.addContainer(container);
      LOG.info("Setting Container [" + container.getId() + "] for task [" + task.getId() + "]..");

      // Add additional environment vars.
      switch (framework) {
        case TENSORFLOW: {
          containerShellEnv.put(Constants.JOB_NAME, task.getJobName());
          containerShellEnv.put(Constants.TASK_INDEX, task.getTaskIndex());
          containerShellEnv.put(Constants.TASK_NUM, String.valueOf(numTotalWorkerTasks));
          break;
        }
        case PYTORCH: {
          containerShellEnv.put(Constants.RANK, task.getTaskIndex());
          containerShellEnv.put(Constants.WORLD, String.valueOf(numTotalWorkerTasks));
          break;
        }
      }

      List<String> commands = new ArrayList<>();

      List<CharSequence> arguments = new ArrayList<>(5);
      arguments.add(session.getTaskCommand());
      arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : arguments) {
        command.append(str).append(" ");
      }
      commands.add(command.toString());

      LOG.info("Constructed command: " + commands);
      LOG.info("Container environment: " + containerShellEnv);


      // Set logs to be readable by everyone.
      Map<ApplicationAccessType, String> acls = new HashMap<>(2);
      acls.put(ApplicationAccessType.VIEW_APP, "*");
      acls.put(ApplicationAccessType.MODIFY_APP, " ");

      ByteBuffer tokens = null;
      if (secureMode) {
        tokens = allTokens.duplicate();
      }
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(containerResources, containerShellEnv,
                                                                      commands, null, tokens, acls);

      String sessionId = String.valueOf(session.sessionId);
      sessionContainersMap.computeIfAbsent(sessionId, key ->
          Collections.synchronizedList(new ArrayList<>())
      ).add(container);

      Utils.printTaskUrl(task.getTaskUrl(), LOG);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  private void onTaskDeemedDead(TonyTask task) {
    LOG.info("Task with id [" + task.getId() + "] has missed"
        + " [" + maxConsecutiveHBMiss + "] heartbeats.. Ending application !!");
    // TODO: figure out what is the right thing to do here..
    // TODO: For the time being, we just kill the job..
    LOG.error("Task with id [" + task.getId() + "] deemed dead!!");
    taskHasMissesHB = true;
    mainThread.interrupt();
  }

  //region testing

  private void killChiefWorkerIfTesting(String taskId) {
    // Simulation of chief worker been killed.
    if (System.getenv(Constants.TEST_WORKER_TERMINATED) != null && taskId.equals(COORDINATOR_ID)) {
      List<Container> containers = sessionContainersMap.get(String.valueOf(session.sessionId));
      for (Container container : containers) {
        if (session.getTask(container.getId()).getJobName().equals(Constants.WORKER_JOB_NAME)) {
          LOG.warn("Simulating worker termination for taskId: " + taskId);
          nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
        }
      }
    }
  }

  //endregion
}
