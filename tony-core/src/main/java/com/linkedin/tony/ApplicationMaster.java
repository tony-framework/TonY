/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.linkedin.tony.dashboard.DashboardHttpServer;
import com.linkedin.tony.events.TaskFinished;
import com.linkedin.tony.events.TaskStarted;
import com.linkedin.tony.models.JobMetadata;
import com.linkedin.tony.events.ApplicationFinished;
import com.linkedin.tony.events.ApplicationInited;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventHandler;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.rpc.ApplicationRpc;
import com.linkedin.tony.rpc.ApplicationRpcServer;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.impl.MetricsRpcServer;
import com.linkedin.tony.rpc.impl.TaskStatus;
import com.linkedin.tony.models.JobContainerRequest;
import com.linkedin.tony.TonySession.TonyTask;
import com.linkedin.tony.util.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.UTCClock;


public class ApplicationMaster {
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  /**
   * Metadata + History Server related variables
   */
  private String appIdString;
  private String applicationName;
  private FileSystem resourceFs; // FileSystem used to access resources for the job, like jars and zips
  private FileSystem historyFs;  // FileSystem used to write history-related files like config and events.
                                 // In some HDFS setups, operators may wish to write history files to a different
                                 // NameNode instance than data and other files.
  private String tonyHistoryFolder;
  private Path jobDir = null;
  private String user = null;
  private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

  // Container info
  private int amRetryCount;
  private long executionTimeout;
  private String hdfsClasspath;
  private int amPort;
  private ByteBuffer allTokens;
  private Map<String, LocalResource> localResources = new ConcurrentHashMap<>();
  private Configuration tonyConf = new Configuration(false);
  private ContainerId containerId;
  private int numAMRetries = 0;

  /** The environment set up for the TaskExecutor **/
  private Map<String, String> containerEnv = new ConcurrentHashMap<>();

  /** The environment passed from users to the training job. Note this is very different from the above. **/
  private Map<String, String> shellEnv = new HashMap<>();

  /** Map of session to containers */
  private Map<Integer, List<Container>> sessionContainersMap = new ConcurrentHashMap<>();

  private Map<String, Map<String, LocalResource>> jobTypeToContainerResources = new HashMap<>();

  /** Node manager delegate **/
  private NMClientAsync nmClientAsync;
  private ExecutorService containersLauncherThreadPool = Executors.newCachedThreadPool();
  /** Resource manager **/
  private AMRMClientAsync<ContainerRequest> amRMClient;

  /** Tony session **/
  private TonySession session = new TonySession(); // Create a dummy session for single node training.
  private TonySession.Builder sessionBuilder;

  /** Configuration **/
  private Configuration yarnConf;
  private Configuration hdfsConf;

  /** Cluster spec **/
  private ApplicationRpcServer applicationRpcServer;

  /** Set to false when testing locally / running in insecure cluster **/
  private boolean secureMode;

  /** Single node training **/
  private boolean singleNode;
  private boolean preprocessFinished = false;
  private int preprocessExitCode = 0;
  private String proxyUrl;

  /** Preprocessing job **/
  private boolean enablePreprocessing = false;

  /** Untracked jobs **/
  private volatile boolean untrackedTaskFailed = false;

  /** Lifecycle control **/
  private long appTimeout;

  /** We use this to give the client a chance to get task updates before the AM shuts down. */
  private volatile boolean clientSignalToStop = false; // client signal to stop

  /** Metrics and events **/
  private MetricsRpcServer metricsRpcServer;
  private EventHandler eventHandler;

  /** HeartBeat monitor **/
  private final AbstractLivelinessMonitor<TonyTask> hbMonitor;
  private int hbInterval;
  private int maxConsecutiveHBMiss;

  /** Task Scheduler **/
  private TaskScheduler scheduler;

  /** Distributed mode**/
  private TonyConfigurationKeys.DistributedMode distributedMode;

  /** Container registration timeout time **/
  private long registrationTimeoutMs;

  /** AM waiting timeout of client signal stop **/
  private int waitingClientSignalStopTimeout;

  /** Framework type, like tensorflow/pytorch/horovod **/
  private String frameworkType;
  private Framework.ApplicationMasterAdapter amRuntimeAdapter;

  /** The web dashboard */
  private DashboardHttpServer dashboardHttpServer;

  private ApplicationMaster() {
    hdfsConf = new Configuration(false);
    yarnConf = new Configuration(false);

    hbMonitor = new AbstractLivelinessMonitor<TonyTask>("Tony Task liveliness Monitor", new UTCClock()) {
      @Override
      protected void expire(TonyTask task) {
        onTaskDeemedDead(task);
      }

      @Override
      protected void serviceStart() throws Exception {
        // setMonitorInterval(int) changed to setMonitorInterval(long) in Hadoop 2.9,
        // so to support both cases, we use reflection
        int monitorInterval = hbInterval * 3;
        for (Method m : this.getClass().getDeclaredMethods()) {
          if (m.getName().equals(Constants.SET_MONITOR_INTERVAL_METHOD)) {
            m.invoke(this, monitorInterval);
            break;
          }
        }
        setExpireInterval(hbInterval * Math.max(3, maxConsecutiveHBMiss)); // Be at least == monitoring interval
        super.serviceStart();
      }
    };
  }

  /**
   * Parse command line options and initialize ApplicationMaster
   * @return whether the initialization is successful or not.
   */
  private boolean init(String[] args) {
    tonyConf.addResource(new Path(Constants.TONY_FINAL_XML));

    Utils.initYarnConf(yarnConf);
    Utils.initHdfsConf(hdfsConf);

    try {
      resourceFs = FileSystem.get(hdfsConf);
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
    String[] shellEnvs = tonyConf.getStrings(TonyConfigurationKeys.EXECUTION_ENV);
    shellEnv = Utils.parseKeyValue(shellEnvs);
    String[] containerEnvs = tonyConf.getStrings(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV);
    containerEnv.putAll(Utils.parseKeyValue(containerEnvs));

    appTimeout = tonyConf.getInt(TonyConfigurationKeys.APPLICATION_TIMEOUT,
                                 TonyConfigurationKeys.DEFAULT_APPLICATION_TIMEOUT);
    executionTimeout = tonyConf.getInt(TonyConfigurationKeys.TASK_EXECUTION_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_TASK_EXECUTION_TIMEOUT);
    hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");
    amRetryCount = tonyConf.getInt(TonyConfigurationKeys.AM_RETRY_COUNT,
        TonyConfigurationKeys.DEFAULT_AM_RETRY_COUNT);
    singleNode = Utils.getNumTotalTasks(tonyConf) == 0;
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
    tonyHistoryFolder = tonyConf.get(TonyConfigurationKeys.TONY_HISTORY_LOCATION,
                                     TonyConfigurationKeys.DEFAULT_TONY_HISTORY_LOCATION);
    String distributedModeVal = tonyConf.get(TonyConfigurationKeys.APPLICATION_DISTRIBUTED_MODE,
            TonyConfigurationKeys.DEFAULT_APPLICATION_DISTRIBUTED_MODE);
    distributedMode = TonyConfigurationKeys.DistributedMode.valueOf(distributedModeVal.toUpperCase());
    registrationTimeoutMs = tonyConf.getInt(TonyConfigurationKeys.CONTAINER_REGISTRATION_TIMEOUT,
            TonyConfigurationKeys.DEFAULT_CONTAINER_REGISTRATION_TIMEOUT);

    waitingClientSignalStopTimeout = tonyConf.getInt(TonyConfigurationKeys.AM_WAIT_CLIENT_STOP_TIMEOUT,
                                                  TonyConfigurationKeys.DEFAULT_AM_WAIT_CLIENT_STOP_TIMEOUT);

    frameworkType = tonyConf.get(TonyConfigurationKeys.FRAMEWORK_NAME,
            TonyConfigurationKeys.DEFAULT_FRAMEWORK_NAME).toUpperCase();
    amRuntimeAdapter = FrameworkRuntimeProvider.getAMAdapter(frameworkType);

    applicationName = tonyConf.get(TonyConfigurationKeys.APPLICATION_NAME,
            TonyConfigurationKeys.DEFAULT_APPLICATION_NAME);

    if (!amRuntimeAdapter.validateAndUpdateConfig(tonyConf)) {
      LOG.error("Invalid TonY conf.");
      return false;
    }

    // Set an environment variable to pass the appid
    containerEnv.put(Constants.APPID, appIdString);

    try {
      historyFs = new Path(tonyHistoryFolder).getFileSystem(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to create history FileSystem object", e);
      return false;
    }

    eventHandler = new EventHandler(historyFs, eventQueue);

    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Failed to fetch users", e);
    }
    return true;
  }

  private void buildTonySession() {
    TonySession.Builder builder = new TonySession.Builder()
        .setTonyConf(tonyConf)
        .setTaskExecutorJVMArgs(tonyConf.get(TonyConfigurationKeys.TASK_EXECUTOR_JVM_OPTS,
            TonyConfigurationKeys.DEFAULT_TASK_EXECUTOR_JVM_OPTS));
    sessionBuilder = builder;
    session = builder.build();
    if (dashboardHttpServer != null) {
      dashboardHttpServer.updateTonySession(session);
    }
  }

  /**
   * Entry point of ApplicationMaster
   * The workflow of a training job in AM
   * prepare -> start -> failed    -> reset -> retry if amRetryCount > 0 otherwise fail the job.
   *                  -> succeeded -> stop -> job succeeded
   * @param args the args from user inputs
   */
  public static void main(String[] args) {
    int exitCode = -1;
    ApplicationMaster am = null;
    try {
      am = new ApplicationMaster();
      boolean succeeded = am.run(args);
      if (succeeded) {
        LOG.info("Application Master completed successfully. Exiting");
        exitCode = 0;
      } else {
        LOG.info("Application Master failed. Exiting");
      }
    } catch (Exception e) {
      LOG.error("AM crashed.", e);
      if (am != null) {
        try {
          am.stop();
        } catch (Exception exception) {
          LOG.error("Errors on clearing up running containers.", exception);
        }
      }
    }
    System.exit(exitCode);
  }

  private boolean run(String[] args) throws IOException {
    long started = System.currentTimeMillis();
    if (!init(args)) {
      return false;
    }

    if (!prepare()) {
      return false;
    }

    // Set up the builder with parameters that don't change
    JobMetadata.Builder metadataBuilder = new JobMetadata.Builder()
        .setId(appIdString)
        .setConf(yarnConf)
        .setStarted(started)
        .setUser(user);
    JobMetadata metadata = metadataBuilder.build();

    if (!eventHandler.setUpThread(jobDir, metadata)) {
      return false;
    }

    eventHandler.start();
    boolean succeeded;
    do {
      // Crash AM on purpose during AM crash tests.
      String shouldCrash = System.getenv(Constants.TEST_AM_CRASH);
      if (shouldCrash != null && shouldCrash.equals("true")) {
        LOG.fatal("Error running ApplicationMaster !!");
        return false;
      }

      // AM throw exception during AM crash tests.
      String throwExceptionCrash = System.getenv(Constants.TEST_AM_THROW_EXCEPTION_CRASH);
      if (throwExceptionCrash != null && throwExceptionCrash.equals("true")) {
        throw new IOException("AM crashed.");
      }

      try {
        eventHandler.emitEvent(new Event(EventType.APPLICATION_INITED,
            new ApplicationInited(appIdString, Utils.getNumTotalTasks(tonyConf), Utils.getCurrentHostName(), this.containerId.toString()),
            System.currentTimeMillis()));
        start();
      } catch (Exception e) {
        LOG.error("Exception when we're starting TonyAM", e);
        return false;
      }

      succeeded = monitor();
      if (succeeded || amRetryCount == 0) {
        LOG.info("Result: " + succeeded + ", retry count: " + amRetryCount);
        break;
      }

      // Prepare for retryCount.
      reset();
      LOG.info("Retrying, remaining retry count" + amRetryCount);

      amRetryCount -= 1;

      numAMRetries += 1;

      //Set an environment variable when restarting due to preemption
      containerEnv.put(Constants.NUM_AM_RETRIES, Integer.toString(numAMRetries));
    } while (!singleNode); // We don't retry on single node training.
    // Wait for the worker nodes to finish (The interval between registering the exit code to final exit)
    stop();
    long completed = System.currentTimeMillis();
    printTaskUrls();
    eventHandler.emitEvent(new Event(EventType.APPLICATION_FINISHED,
        new ApplicationFinished(appIdString, session.getNumCompletedTasks(),
            session.getNumFailedTasks(), new ArrayList<>()),
        System.currentTimeMillis()));
    metadata = metadataBuilder
        .setCompleted(completed)
        .setStatus(succeeded ? Constants.SUCCEEDED : Constants.FAILED)
        .build();
    eventHandler.stop(jobDir, metadata);

    return succeeded;
  }

  /**
   * Prepare the application master. This part is shared across different retries.
   */
  private boolean prepare() throws IOException {
    LOG.info("Preparing application master..");

    NMCallbackHandler containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(yarnConf);
    nmClientAsync.start();

    // Setup application RPC server
    String amHostname = Utils.getCurrentHostName();
    applicationRpcServer = setupAppRPCService(amHostname);
    containerEnv.put(Constants.AM_HOST, amHostname);
    containerEnv.put(Constants.AM_PORT, Integer.toString(amPort));

    // Setup metrics RPC server.
    metricsRpcServer = new MetricsRpcServer(yarnConf);
    int metricsRpcPort = metricsRpcServer.getMetricRpcPort();
    containerEnv.put(Constants.METRICS_RPC_PORT, Integer.toString(metricsRpcPort));

    // Init AMRMClient
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(yarnConf);
    amRMClient.start();

    RegisterApplicationMasterResponse response;
    String hostNameOrIpFromTokenConf;
    String amHostPort;
    try {
      hostNameOrIpFromTokenConf = Utils.getHostNameOrIpFromTokenConf(yarnConf);
      /**
       * Start the TonY dashboard http server,
       * register the dashboard tracking url to Yarn RM.
       */
      String amLogUrl = Utils.constructContainerUrl(hostNameOrIpFromTokenConf + ":"
              + System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name()), containerId);
      final DashboardHttpServer dashboardHttpServer = DashboardHttpServer.builder()
              .amHostName(hostNameOrIpFromTokenConf)
              .runtimeType(frameworkType)
              .session(session)
              .amLogUrl(amLogUrl)
              .appId(appIdString)
              .build();
      String dashboardHttpUrl = dashboardHttpServer.start();
      this.dashboardHttpServer = dashboardHttpServer;

      response = amRMClient.registerApplicationMaster(amHostname, amPort, dashboardHttpUrl);
      amHostPort = hostNameOrIpFromTokenConf + ":" + amPort;
    } catch (Exception e) {
      LOG.error("Exception while preparing AM", e);
      return false;
    }

    if (secureMode) {
      // Set up secret manager for RPC servers
      ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
      ClientToAMTokenIdentifier identifier = new ClientToAMTokenIdentifier(appAttemptID, user);
      byte[] secret = response.getClientToAMTokenMasterKey().array();
      ClientToAMTokenSecretManager secretManager = new ClientToAMTokenSecretManager(appAttemptID, secret);
      applicationRpcServer.setSecretManager(secretManager);
      metricsRpcServer.setSecretManager(secretManager);

      // create token for application RPC server
      Token<? extends TokenIdentifier> tensorflowClusterToken = new Token<>(identifier, secretManager);
      tensorflowClusterToken.setService(new Text(amHostPort));
      UserGroupInformation.getCurrentUser().addToken(tensorflowClusterToken);

      // create token for metrics RPC server
      Token<? extends TokenIdentifier> metricsToken = new Token<>(identifier, secretManager);
      metricsToken.setService(new Text(hostNameOrIpFromTokenConf + ":" + metricsRpcPort));
      UserGroupInformation.getCurrentUser().addToken(metricsToken);

      setupContainerCredentials();
    }

    try {
      setupJobDir(historyFs, tonyHistoryFolder, appIdString);
      writeConfigFile(historyFs, jobDir);
    } catch (IOException e) {
      LOG.error("Error while setting up history files", e);
      return false;
    }

    LOG.info("Starting application RPC server at: " + amHostPort);
    applicationRpcServer.start();

    LOG.info("Starting metrics RPC server at: " + amHostname + ":" + metricsRpcPort);
    metricsRpcServer.start();

    hbMonitor.start();

    return true;
  }

  /**
   * Create job directory under intermediate folder.
   * @param fs FileSystem object.
   * @param histFolder History folder location string.
   * @param appId Application ID string.
   */
  private void setupJobDir(FileSystem fs, String histFolder, String appId) {
    Path interm = new Path(histFolder, Constants.TONY_HISTORY_INTERMEDIATE);
    try {
      if (!fs.exists(interm)) {
        LOG.error("Intermediate directory doesn't exist [" + interm.toString() + "]");
        return;
      }
    } catch (IOException e) {
      LOG.error("Failed to check intermediate directory existence", e);
      return;
    }

    jobDir = new Path(interm, appId);
    // set to `tony` group by default
    // due to inherited permission from parent folder
    Utils.createDirIfNotExists(fs, jobDir, Constants.PERM770);
  }

  /**
   * Generate config file in {@code jobDir} folder.
   * @param fs FileSystem object.
   * @param jobDir Path object of job directory (store all the files related to the job).
   * @throws IOException when failed to write config.xml to {@code jobDir}
   */
  private void writeConfigFile(FileSystem fs, Path jobDir) throws IOException {
    if (jobDir == null) {
      return;
    }
    Path configFile = new Path(jobDir, Constants.TONY_FINAL_XML);
    try (FSDataOutputStream out = fs.create(configFile)) {
      tonyConf.writeXml(out);
    } catch (IOException e) {
      throw new IOException("Failed to write config to XML", e);
    }
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

    buildTonySession();
    session.setResources(yarnConf, hdfsConf, localResources, containerEnv, hdfsClasspath);
    scheduler = new TaskScheduler(session, amRMClient, localResources, resourceFs, tonyConf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    amRuntimeAdapter.setTonySession(session);
  }

  // Reset state to prepare for retryCount.
  private void reset() {
    List<Container> containers = sessionContainersMap.get(session.sessionId);
    for (Container container : containers) {
      nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
      LOG.info("Stop a task in container: containerId = " + container.getId() + ", containerNode = "
               + container.getNodeId().getHost());
    }

    // Reset the flags that indicate failure.
    untrackedTaskFailed = false;

    // Reset session
    session = sessionBuilder.build();
    applicationRpcServer.reset();
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
    int counter = 0;
    while (true) {
      counter += 1;
      // Checking timeout
      if (System.currentTimeMillis() > expireTime) {
        LOG.error("Application times out.");
        break;
      }

      // Check if client signals we should exit.
      if (clientSignalToStop) {
        LOG.info("Client signals AM to exit.");
        break;
      }

      if (session.isTrainingFinished()) {
        LOG.info("Training has finished.");
        break;
      }

      if (preprocessExitCode != 0) {
        LOG.error("Preprocess failed with exit code: " + preprocessExitCode);
        break;
      }

      if (singleNode && preprocessFinished) {
        LOG.info("Single node training finished with exit code: " + preprocessExitCode);
        break;
      }

      if (this.untrackedTaskFailed) {
        LOG.error("One of the untracked tasks has failed with a non-zero exit code.");
        break;
      }

      if (!this.scheduler.dependencyCheckPassed) {
        LOG.info("Terminating application due to failure to load dependency graph");
        break;
      }

      if (!amRuntimeAdapter.isHealthy(tonyConf)) {
        LOG.error("Application failed due to the runtime unhealthy.");
        break;
      }

      // Handle executor registered time out
      if (registrationTimeout()) {
        LOG.error("Application failed due to registered executor task timeout");
        break;
      }

      // Handle executor exit when launching task executor process
      if (startupFailed()) {
        LOG.error("Application failed due to started executor failed.");
        break;
      }

      int numTotalTrackedTasks = session.getTotalTrackedTasks();
      if (numTotalTrackedTasks > 0) {
        int numCompletedTrackedTasks = session.getNumCompletedTrackedTasks();
        if (numCompletedTrackedTasks == numTotalTrackedTasks) {
          Utils.printCompletedTrackedTasks(numCompletedTrackedTasks, numTotalTrackedTasks);
          break;
        }

        // Reduce logging frequency to every 100s.
        if (counter % 20 == 1) {
          Utils.printCompletedTrackedTasks(numCompletedTrackedTasks, numTotalTrackedTasks);
        }
      }

      // Pause before refresh job status
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted", e);
      }
    }

    session.updateSessionStatus();
    FinalApplicationStatus status = session.getFinalStatus();
    String appMessage = session.getFinalMessage();
    if (status != FinalApplicationStatus.SUCCEEDED) {
      LOG.info("Tony session failed: " + appMessage);
    }
    return status == FinalApplicationStatus.SUCCEEDED;
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
    stopRunningContainers();

    FinalApplicationStatus status = session.getFinalStatus();
    String appMessage = session.getFinalMessage();
    try {
      amRMClient.unregisterApplicationMaster(status, appMessage, null);
    } catch (YarnException | IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRuntimeAdapter.destroy();
    nmClientAsync.stop();
    amRMClient.stop();
    // Poll until TonyClient signals we should exit
    boolean result = Utils.poll(() -> clientSignalToStop, 1, waitingClientSignalStopTimeout);
    if (!result) {
      LOG.warn("TonyClient didn't signal Tony AM to stop.");
    }
  }

  /**
   * Stops any remaining running containers and gives them time to finish so we can collect their task metrics and emit
   * a TASK_FINISHED event.
   */
  private void stopRunningContainers() {
    List<Container> allContainers = sessionContainersMap.get(session.sessionId);
    if (allContainers != null) {
      for (Container container : allContainers) {
        TonyTask task = session.getTask(container.getId());
        if (task != null && !task.isCompleted()) {
          nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
        }
      }
    }

    // Give 15 seconds for containers to exit
    boolean result = Utils.poll(() -> session.getNumCompletedTasks() == session.getTotalTasks(), 1, 15);
    if (!result) {
      LOG.warn("Not all containers were stopped or completed. Only " + session.getNumCompletedTasks() + " out of "
          + session.getTotalTasks() + " finished.");
    }
  }

  // Run the preprocessing job and set up the common env variables for worker jobs.
  private int doPreprocessingJob() throws Exception {

    Utils.extractResources(appIdString);
    HashMap<String, String> extraEnv = new HashMap<>(shellEnv);
    if (singleNode) {
      ServerSocket tbSocket = new ServerSocket(0);
      int tbPort = tbSocket.getLocalPort();
      extraEnv.put(Constants.TB_PORT, String.valueOf(tbPort));
      String tbUrl = Utils.getCurrentHostName() + ":" + tbPort;
      proxyUrl = Utils.constructUrl(tbUrl);
      LOG.info("Registering TensorBoard url for single node training: " + tbUrl);
      registerTensorBoardUrl(tbUrl);
      tbSocket.close();
    }
    LOG.info("Start python preprocessing");

    extraEnv.put(Constants.PREPROCESSING_JOB, "true");

    /*
     * YARN sets $HOME to /user/yarn which users don't have access to write there.
     * Unfortunately, some services like Jupyter Notebook wants to write stuff there,
     * set it to user.dir (root of this container's address).
     */
    extraEnv.put("HOME", System.getProperty("user.dir"));
    String taskCommand = tonyConf.get(TonyConfigurationKeys.getExecuteCommandKey(Constants.AM_NAME),
                tonyConf.get(TonyConfigurationKeys.getContainerExecuteCommandKey()));
    LOG.info("Executing command: " + taskCommand);
    int exitCode = Utils.executeShell(taskCommand, executionTimeout, extraEnv);

    preprocessExitCode = exitCode;
    preprocessFinished = true;

    // Short circuit if preprocessing job fails.
    if (exitCode != 0) {
      LOG.error("Preprocess job exits with " + exitCode + ", exiting.");
      session.setFinalStatus(FinalApplicationStatus.FAILED, "Preprocessing job failed.");
      return exitCode;
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(
        System.getProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR)
        + File.separatorChar + Constants.AM_STDOUT_FILENAME), StandardCharsets.UTF_8))) {
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
          .forEach(task -> {
            if (task != null) {
              Utils.printTaskUrl(task.getTaskInfo(), LOG);
            }
          });
    }
  }

  private ApplicationRpcServer setupAppRPCService(String hostname) throws IOException {
    ApplicationRpcServer rpcServer = new ApplicationRpcServer(hostname, new RpcForClient(), yarnConf);
    amPort = rpcServer.getRpcPort();
    return rpcServer;
  }

  private final class RpcForClient implements ApplicationRpc {
    @Override
    public void reset() {
      session.resetRegisteredTasks();
    }

    @Override
    public void registerCallbackInfo(String taskId, String callbackInfo) throws YarnException, IOException {
      if (!amRuntimeAdapter.receiveTaskCallbackInfo(taskId, callbackInfo)) {
        LOG.error("Errors on receiving task executors' callbaclk info. task id: "
                + taskId + ", callback info: " + callbackInfo);
      }
    }

    @Override
    public Set<TaskInfo> getTaskInfos() {
      // Special handling for NotebookSubmitter.
      if (singleNode && proxyUrl != null) {
        HashSet<TaskInfo> additionalTasks = new HashSet<>();
        additionalTasks.add(new TaskInfo(Constants.DRIVER_JOB_NAME, "0", Utils.constructContainerUrl(
                          Utils.getCurrentHostName() + ":"
                          + System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name()), containerId)));
        additionalTasks.add(new TaskInfo(Constants.NOTEBOOK_JOB_NAME, "0", proxyUrl));
        return additionalTasks;
      }

      if (!singleNode && session != null && session.allTasksScheduled()) {
        return session.getTonyTasks().values().stream()
            .flatMap(tasks -> Arrays.stream(tasks).map(TonyTask::getTaskInfo))
            .collect(Collectors.toSet());
      }

      return Collections.emptySet();
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
    public String getClusterSpec(String taskId) throws IOException {
      if (amRuntimeAdapter.canStartTask(distributedMode, taskId)) {
        return amRuntimeAdapter.constructClusterSpec(taskId);
      }
      return StringUtils.EMPTY;
    }

    @Override
    public String registerWorkerSpec(String taskId, String spec) throws IOException {
      TonyTask task = session.getTask(taskId);
      if (task.getHost() == null) {
        LOG.info("Received cluster spec registration request from task " + taskId + " with spec: " + spec);
        task.setHostPort(spec);
        session.addRegisteredTask(taskId);

        // HB Registration should happen only after worker registration..
        // The Task registration timeout will take care of rescheduling the task
        // on another node..
        LOG.info("[" + taskId + "] Received Registration for HB !!");
        hbMonitor.register(task);
        killChiefWorkerIfTesting(taskId);
        return StringUtils.EMPTY;
      }
      return null;
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
     * of a task. However, we use this method to unregister a completed task from the heartbeat monitor to avoid a race
     * condition where the container complete callback is delayed, too many heartbeats are missed, and the task is
     * marked as failed.
     */
    @Override
    public String registerExecutionResult(int exitCode, String jobName, String jobIndex, String sessionId) {
      LOG.info("Received result registration request with exit code " + exitCode + " from " + jobName + " " + jobIndex);

      // Unregister task after completion..
      // Since in the case of asynchronous exec, containers might
      // end at different times..
      TonyTask task = session.getTask(jobName + ":" + jobIndex);
      if (task != null) {
        LOG.info("Unregistering task [" + task.getId() + "] from Heartbeat monitor..");
        hbMonitor.unregister(task);
      } else {
        LOG.warn("Task " + jobName + " " + jobIndex + " was null!");
      }
      return "RECEIVED";
    }


    @Override
    public String registerTensorBoardUrl(String spec) throws Exception {
      LOG.info("Got request to update TensorBoard URL: " + spec);
      return ApplicationMaster.this.registerTensorBoardUrl(spec);
    }

    @Override
    public void finishApplication() {
      LOG.info("Client signals AM to finish application.");
      clientSignalToStop = true;
    }
  }

  private String registerTensorBoardUrl(String tbUrl) throws Exception {
    if (dashboardHttpServer != null) {
      dashboardHttpServer.registerTensorboardUrl(tbUrl);
    }
    return "SUCCEEDED";
  }

  // Set up credentials for the containers.
  private void setupContainerCredentials() throws IOException {
    Credentials amCred = UserGroupInformation.getCurrentUser().getCredentials();

    Credentials containersCred = new Credentials();
    for (Token<? extends TokenIdentifier> token : amCred.getAllTokens()) {
      if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        containersCred.addToken(token.getService(), token);
      }
    }

    // To support Hadoop transparent encryption, the secret key also needs to be passed
    // to the containers so the encrypted content can be decrypted.
    for (Text alias : amCred.getAllSecretKeys()) {
      containersCred.addSecretKey(alias, amCred.getSecretKey(alias));
    }

    DataOutputBuffer dob = new DataOutputBuffer();
    containersCred.writeTokenStorageToStream(dob);
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    String submitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation submitterUgi = UserGroupInformation.createRemoteUser(submitterUserName);
    submitterUgi.addCredentials(containersCred);
  }

  private NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler();
  }

  /**
   * Node manager call back handler
   */
  class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    @Override
    public void onContainerStopped(ContainerId containerId) {
      processFinishedContainer(containerId, ContainerExitStatus.KILLED_BY_APPMASTER, "KILLED_BY_APPMASTER");
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
      LOG.error("Failed to start container " + containerId, t);
      processFinishedContainer(containerId, ContainerExitStatus.INVALID,
              "Errors on starting container, stacktrace as follows: \n" + ExceptionUtils.getStackTrace(t));
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of container " + containerId, t);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop container " + containerId, t);
    }
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("onContainersCompleted called in RMCallbackHandler, completed containers size: " + completedContainers.size());
      sleepForTesting();

      for (ContainerStatus containerStatus : completedContainers) {
        int exitStatus = containerStatus.getExitStatus();
        String diagnostics = containerStatus.getDiagnostics();
        String outputLog = "ContainerID = " + containerStatus.getContainerId()
                + ", state = " + containerStatus.getState()
                + ", exitStatus = " + exitStatus
                + ", diagnostics = " + diagnostics;

        String errorInformation = null;
        if (ContainerExitStatus.SUCCESS != exitStatus) {
          errorInformation = diagnostics;
          LOG.error(outputLog);
        } else {
          LOG.info(outputLog);
        }
        processFinishedContainer(containerStatus.getContainerId(), exitStatus, errorInformation);
      }
    }

    /**
     * For testing purposes to simulate delay of container completion callback.
     */
    private void sleepForTesting() {
      if (System.getenv(Constants.TEST_TASK_COMPLETION_NOTIFICATION_DELAYED) != null) {
        LOG.info("Sleeping for 1 second to simulate task completion notification delay");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while sleeping", e);
        }
      }
    }

    private String getNodeLabelsExpression(int priority) {
      final List<JobContainerRequest> requests = session.getContainersRequests();
      for (JobContainerRequest request : requests) {
        if (request.getPriority() == priority) {
          return request.getNodeLabelsExpression();
        }
      }
      return null;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Allocated: " + containers.size() + " containers.");
      for (Container container : containers) {
        // Need to explicitly remove container requests from remoteRequestsTable in AMRMClient, otherwise
        // resources get double-requested (YARN-1902)
        amRMClient.removeContainerRequest(Utils.setupContainerRequestForRM(new JobContainerRequest(
            "", 1, HadoopCompatibleAdapter.getMemorySize(container.getResource()),
            container.getResource().getVirtualCores(),
            HadoopCompatibleAdapter.getNumOfRequestedGPU(container),
            container.getPriority().getPriority(),
            getNodeLabelsExpression(container.getPriority().getPriority()),
            new ArrayList<>())));
        LOG.info("Launching a task in container"
            + ", containerId = " + container.getId()
            + ", containerNode = " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
            + ", resourceRequest = " + container.getResource()
            + ", priority = " + container.getPriority());
        containersLauncherThreadPool.execute(new ContainerLauncher(container));
      }
    }

    @Override
    public void onShutdownRequest() {
      LOG.info("onShutdownRequest called in RMCallbackHandler");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
      StringBuilder consoleMsgBuilder =
              new StringBuilder("Received onNodesUpdated called in RMCallbackHandler, node reports as follows.");
      if (list == null || list.size() == 0) {
        LOG.info(consoleMsgBuilder.toString());
        return;
      }
      for (NodeReport nodeReport : list) {
        consoleMsgBuilder.append("\nnodeId = " + nodeReport.getNodeId() + ", healthReport = " + nodeReport.getHealthReport());
      }
      LOG.info(consoleMsgBuilder.toString());
    }

    @Override
    public float getProgress() {
      int numTotalTrackedTasks = session.getTotalTrackedTasks();
      return numTotalTrackedTasks > 0 ? (float) session.getNumCompletedTrackedTasks() / numTotalTrackedTasks : 0;
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("Received error in AM to RM call", throwable);
      session.setFinalStatus(FinalApplicationStatus.FAILED, throwable.getMessage());
      stop();
    }
  }

  /**
   * The command to prepare inside containers.
   */
  private class ContainerLauncher implements Runnable {
    Container container;

    ContainerLauncher(Container container) {
      this.container = container;
    }

    /**
     * Set up container's launch command and start the container.
     */
    public void run() {
      TonyTask task = session.getAndInitMatchingTaskByPriority(container.getPriority().getPriority());
      Preconditions.checkNotNull(task, "Task was null! Nothing to schedule.");

      task.setTaskInfo(container);
      TaskInfo taskInfo = task.getTaskInfo();
      taskInfo.setStatus(TaskStatus.READY);

      // Add job type specific resources
      Map<String, LocalResource> containerResources = jobTypeToContainerResources.get(task.getJobName());

      task.addContainer(container);
      LOG.info("Setting Container [" + container.getId() + "] for task [" + task.getId() + "]..");

      Map<String, String> containerLaunchEnv = new ConcurrentHashMap<>(containerEnv);

      /*
       * Add additional environment vars. We always set job_name task_index & task_num and
       * task_num and TaskExecutor is responsible for setting up the actual shell environment
       * for different deep learning frameworks.
       */
      String jobName = task.getJobName();
      String taskIndex = task.getTaskIndex();
      Map<String, String> dockerEnv = HadoopCompatibleAdapter.getContainerEnvForDocker(tonyConf, jobName);
      containerLaunchEnv.putAll(dockerEnv);
      containerLaunchEnv.put(Constants.JOB_NAME, jobName);
      containerLaunchEnv.put(Constants.JOB_ID, appIdString);
      containerLaunchEnv.put(Constants.TASK_INDEX, taskIndex);
      containerLaunchEnv.put(Constants.TASK_NUM, String.valueOf(session.getTotalTrackedTasks()));
      containerLaunchEnv.put(Constants.DISTRIBUTED_MODE_NAME, distributedMode.name());
      if (session.isChief(jobName, taskIndex)) {
        containerLaunchEnv.put(Constants.IS_CHIEF, Boolean.TRUE.toString());
      }
      // Specify session id in the env to distinguish between different sessions.
      containerLaunchEnv.put(Constants.SESSION_ID, String.valueOf(session.sessionId));

      List<CharSequence> arguments = new ArrayList<>(5);
      arguments.add(session.getTaskCommand(appIdString, applicationName, jobName, taskIndex));
      arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      List<String> commands = ImmutableList.of(String.join(" ", arguments));

      LOG.info("Constructed command: " + commands);
      LOG.info("Container environment: " + containerLaunchEnv);

      // Set logs to be readable by everyone.
      Map<ApplicationAccessType, String> acls = new HashMap<>(2);
      acls.put(ApplicationAccessType.VIEW_APP, "*");
      acls.put(ApplicationAccessType.MODIFY_APP, " ");

      ByteBuffer tokens = null;
      if (secureMode) {
        tokens = allTokens.duplicate();
      }
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(containerResources, containerLaunchEnv,
                                                                      commands, null, tokens, acls);

      sessionContainersMap.computeIfAbsent(session.sessionId, key ->
          Collections.synchronizedList(new ArrayList<>())
      ).add(container);

      Utils.printTaskUrl(task.getTaskInfo(), LOG);
      nmClientAsync.startContainerAsync(container, ctx);
      taskInfo.setStatus(TaskStatus.RUNNING);
      eventHandler.emitEvent(new Event(EventType.TASK_STARTED,
          new TaskStarted(task.getJobName(), Integer.parseInt(task.getTaskIndex()),
              container.getNodeHttpAddress().split(":")[0], container.getId().toString()),
          System.currentTimeMillis()));
    }
  }

  private void onTaskDeemedDead(TonyTask task) {
    String msg = "Task with id [" + task.getId() + "] has missed"
        + " [" + maxConsecutiveHBMiss + "] heartbeats. Ending application!";
    LOG.error(msg);
    handleFinishedTask(task, ContainerExitStatus.INVALID, msg);
  }

  private void handleFinishedTask(TonyTask task, int exitStatus, String diagnosticMsg) {
    // Ignore tasks from past sessions.
    if (task.getSessionId() != session.sessionId) {
      return;
    }
    session.onTaskCompleted(task.getJobName(), task.getTaskIndex(), exitStatus, diagnosticMsg);
    hbMonitor.unregister(task);

    scheduler.registerDependencyCompleted(task.getJobName());
    if (ContainerExitStatus.SUCCESS != exitStatus) {
      eventHandler.emitEvent(new Event(EventType.TASK_FINISHED,
              new TaskFinished(task.getJobName(), Integer.parseInt(task.getTaskIndex()),
                      task.getTaskInfo().getStatus().toString(),
                      metricsRpcServer.getMetrics(task.getJobName(), Integer.parseInt(task.getTaskIndex())),
                      diagnosticMsg), System.currentTimeMillis()));
    } else {
      eventHandler.emitEvent(new Event(EventType.TASK_FINISHED,
              new TaskFinished(task.getJobName(), Integer.parseInt(task.getTaskIndex()),
                      task.getTaskInfo().getStatus().toString(),
                      metricsRpcServer.getMetrics(task.getJobName(), Integer.parseInt(task.getTaskIndex())), "NA"),
              System.currentTimeMillis()));
    }

    // Detect if an untracked task has crashed to prevent application hangups.
    boolean fastFail = Utils.isUntrackedJobType(task.getJobName(), tonyConf) && task.isFailed();
    if (fastFail) {
      untrackedTaskFailed = true;
    }
  }

  private void processFinishedContainer(ContainerId containerId, int exitStatus, String diagnosticMessage) {
    TonyTask task = session.getTask(containerId);
    if (task != null) {
      LOG.info("Container " + containerId + " for task " + task + " finished with exitStatus " + exitStatus + ".");
      handleFinishedTask(task, exitStatus, diagnosticMessage);
    } else {
      LOG.warn("No task found for container : [" + containerId + "]!");
    }
  }

  private boolean startupFailed() {
    Set<TonyTask> completedFailedTasks = getCompletedFailedTasks();
    LOG.debug("Completed failed task size: " + completedFailedTasks.size());
    LOG.info("Completed failed tasks list:");
    completedFailedTasks.stream().forEach(x -> {
      LOG.info("Jobname: " + x.getJobName() + ", index: " + x.getTaskIndex());
    });

    LOG.debug("Registered tasks list:");
    session.getRegisteredTasks().stream().forEach(x -> {
      LOG.debug(x);
    });

    // When executor failed and not registering to AM, it means task failed when starting task executor process
    return completedFailedTasks.stream().anyMatch(t -> {
      String taskId = t.getTaskInfo().getName() + ":" + t.getTaskInfo().getIndex();
      LOG.debug("Failed task ID: " + taskId);
      boolean existFailed = !session.getRegisteredTasks().contains(taskId);
      if (existFailed) {
        String errorMsg = String.format("Stopping AM for task [%s:%s] starting failed, "
                        + "allocated container is %s on host %s",
                t.getJobName(), t.getTaskIndex(),
                (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
                (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none"));
        LOG.error(errorMsg);
        session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
        stop();
      }
      return existFailed;
    });
  }

  private Set<TonyTask> getCompletedFailedTasks() {
    return session.getTonyTasks().values().stream().flatMap(Arrays::stream)
            .filter(task -> task != null && task.getTaskInfo() != null && task.isFailed())
            .collect(Collectors.toSet());
  }

  private boolean registrationTimeout() {
    if (registrationTimeoutMs <= 0) {
      return false;
    }

    Set<TonyTask> unregisteredTasks = getUnregisteredTasks();
    for (TonyTask t : unregisteredTasks) {
      if (System.currentTimeMillis() - t.getStartTime() <= registrationTimeoutMs) {
        continue;
      }

      // If registration time out, need to stop all containers. And set the app failed
      String errorMsg = String.format("Stopping AM for task [%s:%s] registration timeout: "
                      + "allocated container is %s on host %s",
              t.getJobName(), t.getTaskIndex(),
              (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
              (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none"));
      LOG.error(errorMsg);
      session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
      stop();
      return true;
    }

    return false;
  }

  //region testing

  private void killChiefWorkerIfTesting(String taskId) {
    // Simulation of chief worker been killed.
    if (System.getenv(Constants.TEST_WORKER_TERMINATED) != null && taskId.equals(Constants.COORDINATOR_ID)) {
      List<Container> containers = sessionContainersMap.get(session.sessionId);
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
