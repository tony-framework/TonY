/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.linkedin.tony.client.CallbackHandler;
import com.linkedin.tony.client.TaskUpdateListener;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.impl.ApplicationRpcClient;
import com.linkedin.tony.tensorflow.TensorFlowContainerRequest;
import com.linkedin.tony.util.HdfsUtils;
import com.linkedin.tony.util.Utils;
import com.linkedin.tony.util.VersionInfo;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


/**
 * User entry point to submit tensorflow job.
 */
public class TonyClient implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(TonyClient.class);

  // Configurations
  private YarnClient yarnClient;
  private HdfsConfiguration hdfsConf = new HdfsConfiguration();
  private YarnConfiguration yarnConf = new YarnConfiguration();
  private Options opts;

  // RPC
  private String amHost;
  private int amRpcPort;
  private boolean amRpcServerInitialized = false;
  private ApplicationRpcClient amRpcClient;

  // Containers set up.
  private String hdfsConfAddress = null;
  private String yarnConfAddress = null;
  private long amMemory;
  private int amVCores;
  private int amGpus;
  private String taskParams = null;
  private String pythonBinaryPath = null;
  private String pythonVenv = null;
  private String srcDir = null;
  private Set<String> applicationTags;
  private String hdfsClasspath = null;
  private String executes;
  private long appTimeout;
  private boolean secureMode;
  private Map<String, String> shellEnv = new HashMap<>();
  private Map<String, String> containerEnv = new HashMap<>();

  private String tonyFinalConfPath;
  private Configuration tonyConf;
  private final long clientStartTime = System.currentTimeMillis();
  private ApplicationId appId;
  private Path appResourcesPath;
  private int hbInterval;
  private int maxHbMisses;

  private CallbackHandler callbackHandler;
  private CopyOnWriteArrayList<TaskUpdateListener> listeners = new CopyOnWriteArrayList<>();

  // For access from CLI.
  private Set<TaskInfo> taskInfos = new HashSet<>();

  public TonyClient() {
    this(new Configuration(false));
  }

  public TonyClient(Configuration conf) {
    this(null, conf);
  }

  public TonyClient(CallbackHandler handler, Configuration conf) {
    initOptions();
    callbackHandler = handler;
    tonyConf = conf;
    VersionInfo.injectVersionInfo(tonyConf);
  }

  private boolean run() throws IOException, InterruptedException, URISyntaxException, YarnException, ParseException {
    LOG.info("Starting client..");
    yarnClient.start();
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    long maxMem = appResponse.getMaximumResourceCapability().getMemory();

    // Truncate resource request to cluster's max resource capability.
    if (amMemory > maxMem) {
      LOG.warn("Truncating requested AM memory: " + amMemory + " to cluster's max: " + maxMem);
      amMemory = maxMem;
    }
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();

    if (amVCores > maxVCores) {
      LOG.warn("Truncating requested AM vcores: " + amVCores + " to cluster's max: " + maxVCores);
      amVCores = maxVCores;
    }

    FileSystem fs = FileSystem.get(hdfsConf);
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appId = appContext.getApplicationId();
    if (callbackHandler != null) {
      callbackHandler.onApplicationIdReceived(appId);
    }
    appResourcesPath = new Path(fs.getHomeDirectory(), Constants.TONY_FOLDER + Path.SEPARATOR + appId.toString());

    this.tonyFinalConfPath = processFinalTonyConf();
    submitApplication(appContext);
    return monitorApplication();
  }

  @VisibleForTesting
  public String processFinalTonyConf() throws IOException, ParseException {
    FileSystem fs = FileSystem.get(hdfsConf);
    if (srcDir != null) {
      if (Utils.isArchive(srcDir)) {
        Utils.uploadFileAndSetConfResources(appResourcesPath, new Path(srcDir),
            Constants.TONY_SRC_ZIP_NAME, tonyConf, fs, LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
      } else {
        Utils.zipFolder(Paths.get(srcDir), Paths.get(Constants.TONY_SRC_ZIP_NAME));
        Utils.uploadFileAndSetConfResources(appResourcesPath, new Path(Constants.TONY_SRC_ZIP_NAME),
            Constants.TONY_SRC_ZIP_NAME, tonyConf, fs, LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
      }
    }

    if (pythonVenv != null) {
      Utils.uploadFileAndSetConfResources(appResourcesPath,
          new Path(pythonVenv), Constants.PYTHON_VENV_ZIP, tonyConf, fs, LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
    }


    URL coreSiteUrl = yarnConf.getResource(Constants.CORE_SITE_CONF);
    if (coreSiteUrl != null) {
      Utils.uploadFileAndSetConfResources(appResourcesPath, new Path(coreSiteUrl.getPath()),
          Constants.CORE_SITE_CONF, tonyConf, fs, LocalResourceType.FILE,
          TonyConfigurationKeys.getContainerResourcesKey());
    }

    addConfToResources(yarnConf, yarnConfAddress, Constants.YARN_SITE_CONF);
    addConfToResources(hdfsConf, hdfsConfAddress, Constants.HDFS_SITE_CONF);
    processTonyConfResources(tonyConf, fs);

    String tonyFinalConf = Utils.getClientResourcesPath(appId.toString(), Constants.TONY_FINAL_XML);
    // Write user's overridden conf to an xml to be localized.
    try (OutputStream os = new FileOutputStream(tonyFinalConf)) {
      tonyConf.writeXml(os);
    } catch (IOException exception) {
      LOG.error("Failed to write tony final conf to: " + tonyFinalConf, exception);
    }
    return tonyFinalConf;
  }

  @VisibleForTesting
  public void submitApplication(ApplicationSubmissionContext appContext)
      throws YarnException, IOException, URISyntaxException {

    String appName = tonyConf.get(TonyConfigurationKeys.APPLICATION_NAME,
        TonyConfigurationKeys.DEFAULT_APPLICATION_NAME);
    appContext.setApplicationName(appName);
    appContext.setApplicationType(Constants.APP_TYPE);
    if (!applicationTags.isEmpty()) {
      appContext.setApplicationTags(applicationTags);
    }

    // Set up resource type requirements
    Resource capability = Resource.newInstance((int) amMemory, amVCores);
    Utils.setCapabilityGPU(capability, amGpus);
    appContext.setResource(capability);

    // Set the queue to which this application is to be submitted in the RM
    String yarnQueue = tonyConf.get(TonyConfigurationKeys.YARN_QUEUE_NAME,
        TonyConfigurationKeys.DEFAULT_YARN_QUEUE_NAME);
    appContext.setQueue(yarnQueue);

    // Set the ContainerLaunchContext to describe the Container ith which the ApplicationMaster is launched.
    ContainerLaunchContext amSpec =
        createAMContainerSpec(this.amMemory, getTokens());
    appContext.setAMContainerSpec(amSpec);
    String nodeLabel = tonyConf.get(TonyConfigurationKeys.APPLICATION_NODE_LABEL);
    if (nodeLabel != null) {
      appContext.setNodeLabelExpression(nodeLabel);
    }
    LOG.info("Submitting YARN application");
    yarnClient.submitApplication(appContext);
    ApplicationReport report = yarnClient.getApplicationReport(appId);
    logTrackingAndRMUrls(report);
  }

  /**
   * Uploads a configuration (e.g.: YARN or HDFS configuration) to HDFS and adds the configuration to the
   * container resources in the TonY conf.
   * @param conf  Configuration to upload and add.
   * @param confAddress  If set, this config file will be uploaded rather than the {@code confFileName}.
   * @param confFileName  The name of the config file to upload (if {@code confAddress} is not set, and also
   *                      the name of the config file when localized in the cluster.
   * @throws IOException  if an error occurs while getting a {@link FileSystem} instance
   */
  private void addConfToResources(Configuration conf, String confAddress, String confFileName) throws IOException {
    Path confSitePath = null;
    if (confAddress != null) {
      confSitePath = new Path(confAddress);
    } else {
      URL confSiteUrl = conf.getResource(confFileName);
      if (confSiteUrl != null) {
        confSitePath = new Path(confSiteUrl.getPath());
      }
    }
    if (confSitePath != null) {
      Utils.uploadFileAndSetConfResources(appResourcesPath, confSitePath, confFileName, tonyConf,
          FileSystem.get(hdfsConf), LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
    }
  }

  private void logTrackingAndRMUrls(ApplicationReport report) {
    LOG.info("URL to track running application (will proxy to TensorBoard once it has started): "
             + report.getTrackingUrl());
    LOG.info("ResourceManager web address for application: "
        + Utils.buildRMUrl(yarnConf, report.getApplicationId().toString()));
  }

  private void initHdfsConf() {
    if (System.getenv(Constants.HADOOP_CONF_DIR) != null) {
      hdfsConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.CORE_SITE_CONF));
      hdfsConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.HDFS_SITE_CONF));
    }
    if (hdfsConfAddress != null) {
      hdfsConf.addResource(new Path(hdfsConfAddress));
    }
  }

  private void createYarnClient() {
    if (System.getenv(Constants.HADOOP_CONF_DIR) != null) {
      yarnConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.CORE_SITE_CONF));
      yarnConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.YARN_SITE_CONF));
    }

    if (this.yarnConfAddress != null) {
      this.yarnConf.addResource(new Path(this.yarnConfAddress));
    }

    int numRMConnectRetries = tonyConf.getInt(TonyConfigurationKeys.RM_CLIENT_CONNECT_RETRY_MULTIPLIER,
        TonyConfigurationKeys.DEFAULT_RM_CLIENT_CONNECT_RETRY_MULTIPLIER);
    long rmMaxWaitMS = yarnConf.getLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS) * numRMConnectRetries;
    yarnConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, rmMaxWaitMS);

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
  }

  private void initOptions() {
    opts = Utils.getCommonOptions();
    opts.addOption("executes", true, "The file to execute on workers.");
    opts.addOption("task_params", true, "The task params to pass into python entry point.");
    opts.addOption("shell_env", true, "Environment for shell script, specified as env_key=env_val pairs");
    opts.addOption("container_env", true, "Environment for the worker containers, specified as key=val pairs");
    opts.addOption("conf", true, "User specified configuration, as key=val pairs");
    opts.addOption("conf_file", true, "Name of user specified conf file, on the classpath");
    opts.addOption("src_dir", true, "Name of directory of source files.");
    opts.addOption("help", false, "Print usage");
  }

  private void printUsage() {
    new HelpFormatter().printHelp("TonyClient", opts);
  }

  public boolean init(String[] args) throws ParseException, IOException {
    CommandLine cliParser = new GnuParser().parse(opts, args, true);

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }

    initTonyConf(tonyConf, cliParser);
    validateTonyConf(tonyConf);

    String amMemoryString = tonyConf.get(TonyConfigurationKeys.AM_MEMORY,
        TonyConfigurationKeys.DEFAULT_AM_MEMORY);
    amMemory = Integer.parseInt(Utils.parseMemoryString(amMemoryString));
    amVCores = tonyConf.getInt(TonyConfigurationKeys.AM_VCORES,
        TonyConfigurationKeys.DEFAULT_AM_VCORES);
    amGpus = tonyConf.getInt(TonyConfigurationKeys.AM_GPUS,
        TonyConfigurationKeys.DEFAULT_AM_GPUS);
    secureMode = tonyConf.getBoolean(TonyConfigurationKeys.SECURITY_ENABLED,
        TonyConfigurationKeys.DEFAULT_SECURITY_ENABLED);
    hbInterval = tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS);
    maxHbMisses = tonyConf.getInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS,
        TonyConfigurationKeys.DEFAULT_TASK_MAX_MISSED_HEARTBEATS);

    LOG.info("TonY heartbeat interval [" + hbInterval + "]");
    LOG.info("TonY max heartbeat misses allowed [" + maxHbMisses + "]");

    hdfsConfAddress = tonyConf.get(TonyConfigurationKeys.HDFS_CONF_LOCATION);
    yarnConfAddress = tonyConf.get(TonyConfigurationKeys.YARN_CONF_LOCATION);
    initHdfsConf();
    createYarnClient();

    taskParams = cliParser.getOptionValue("task_params");
    pythonBinaryPath = cliParser.getOptionValue("python_binary_path");
    pythonVenv = cliParser.getOptionValue("python_venv");
    executes = cliParser.getOptionValue("executes");
    executes = TonyClient.buildTaskCommand(pythonVenv, pythonBinaryPath, executes, taskParams);
    if (executes != null) {
      tonyConf.set(TonyConfigurationKeys.getContainerExecuteCommandKey(), executes);
    }

    // src_dir & hdfs_classpath flags are for compatibility.
    srcDir = cliParser.getOptionValue("src_dir");

    applicationTags = new HashSet<>(
        tonyConf.getStringCollection(TonyConfigurationKeys.APPLICATION_TAGS));

    // Set hdfsClassPath for all workers
    // Prepend hdfs:// if missing
    hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");
    if (hdfsClasspath != null && !hdfsClasspath.startsWith(FileSystem.get(hdfsConf).getScheme())) {
      hdfsClasspath = FileSystem.getDefaultUri(hdfsConf) + hdfsClasspath;
    }
    Utils.appendConfResources(TonyConfigurationKeys.getContainerResourcesKey(), hdfsClasspath, tonyConf);

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                                         + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                                         + " Specified virtual cores=" + amVCores);
    }

    boolean singleNode = tonyConf.getBoolean(TonyConfigurationKeys.IS_SINGLE_NODE,
        TonyConfigurationKeys.DEFAULT_IS_SINGLE_NODE);
    if (!singleNode) {
      if (amGpus > 0) {
        LOG.warn("It seems you reserved " + amGpus + " GPUs in application master (driver, which doesn't perform "
            + "training) during distributed training.");
      }
    }

    appTimeout = tonyConf.getInt(TonyConfigurationKeys.APPLICATION_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_APPLICATION_TIMEOUT);

    List<String> executionEnvPair = new ArrayList<>();
    if (tonyConf.get(TonyConfigurationKeys.EXECUTION_ENV) != null) {
      String[] envs = tonyConf.getStrings(TonyConfigurationKeys.EXECUTION_ENV);
      executionEnvPair.addAll(Arrays.asList(envs));
      shellEnv.putAll(Utils.parseKeyValue(envs));
    }
    if (cliParser.hasOption("shell_env")) {
      String[] envs = cliParser.getOptionValues("shell_env");
      executionEnvPair.addAll(Arrays.asList(envs));
      shellEnv.putAll(Utils.parseKeyValue(envs));
    }
    if (!executionEnvPair.isEmpty()) {
      tonyConf.setStrings(TonyConfigurationKeys.EXECUTION_ENV, executionEnvPair.toArray(new String[0]));
    }

    if (!parseDockerConfigs()) {
      return false;
    }

    List<String> containerEnvPair = new ArrayList<>();
    if (tonyConf.get(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV) != null) {
      String[] envs = tonyConf.getStrings(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV);
      containerEnvPair.addAll(Arrays.asList(envs));
      containerEnv.putAll(Utils.parseKeyValue(envs));
    }
    if (cliParser.hasOption("container_env")) {
      String[] containerEnvs = cliParser.getOptionValues("container_env");
      containerEnvPair.addAll(Arrays.asList(containerEnvs));
      containerEnv.putAll(Utils.parseKeyValue(containerEnvs));
    }
    if (!containerEnv.isEmpty()) {
      tonyConf.setStrings(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV, containerEnvPair.toArray(new String[0]));
    }

    return true;
  }

  /**
   * Parses Docker related configs and sets the appropriate container environment variables if Docker is available.
   * Uses reflection to support older versions of Hadoop.
   */
  private boolean parseDockerConfigs() {
    if (tonyConf.getBoolean(TonyConfigurationKeys.DOCKER_ENABLED, TonyConfigurationKeys.DEFAULT_DOCKER_ENABLED)) {
      String imagePath = tonyConf.get(TonyConfigurationKeys.getContainerDockerKey());
      if (tonyConf.get(TonyConfigurationKeys.getDockerImageKey(Constants.AM_NAME)) != null) {
        imagePath = tonyConf.get(TonyConfigurationKeys.getDockerImageKey(Constants.AM_NAME));
      }
      if (imagePath == null) {
        LOG.error("Docker is enabled but " + TonyConfigurationKeys.getContainerDockerKey() + " is not set.");
        return false;
      } else {
        Class containerRuntimeClass;
        Class dockerRuntimeClass;
        try {
          containerRuntimeClass = Class.forName(Constants.CONTAINER_RUNTIME_CONSTANTS_CLASS);
          dockerRuntimeClass = Class.forName(Constants.DOCKER_LINUX_CONTAINER_RUNTIME_CLASS);
        } catch (ClassNotFoundException e) {
          LOG.error("Docker runtime classes not found in this version ("
              + org.apache.hadoop.util.VersionInfo.getVersion() + ") of Hadoop.", e);
          return false;
        }
        if (dockerRuntimeClass != null) {
          try {
            String envContainerType = (String) containerRuntimeClass.getField(Constants.ENV_CONTAINER_TYPE).get(null);
            String envDockerImage = (String) dockerRuntimeClass.getField(Constants.ENV_DOCKER_CONTAINER_IMAGE).get(null);
            containerEnv.put(envContainerType, "docker");
            containerEnv.put(envDockerImage, imagePath);
          } catch (NoSuchFieldException e) {
            LOG.error("Field " + Constants.ENV_CONTAINER_TYPE + " or " + Constants.ENV_DOCKER_CONTAINER_IMAGE + " not "
                + "found in " + containerRuntimeClass.getName());
            return false;
          } catch (IllegalAccessException e) {
            LOG.error("Unable to access " + Constants.ENV_CONTAINER_TYPE + " or "
                + Constants.ENV_DOCKER_CONTAINER_IMAGE + " fields.");
            return false;
          }
        }
      }
    }
    return true;
  }


  @VisibleForTesting
  static String buildTaskCommand(String pythonVenv, String pythonBinaryPath, String execute,
                                 String taskParams) {
    if (execute == null) {
      return null;
    }
    String baseTaskCommand = execute;
    String pythonInterpreter;
    if (pythonBinaryPath != null) {
      if (pythonBinaryPath.startsWith("/") || pythonVenv == null) {
        pythonInterpreter = pythonBinaryPath;
      } else {
        pythonInterpreter = Constants.PYTHON_VENV_DIR + File.separatorChar  + pythonBinaryPath;
      }
      baseTaskCommand = pythonInterpreter + " " + execute;
    }

    if (taskParams != null) {
      baseTaskCommand += " " + taskParams;
    }

    return baseTaskCommand;
  }


  /**
   * Add resource if exist to {@code tonyConf}
   * @param tonyConf Configuration object.
   * @param cliParser CommandLine object that has all the command line arguments.
   */
  public void initTonyConf(Configuration tonyConf, CommandLine cliParser) throws IOException {
    tonyConf.addResource(Constants.TONY_DEFAULT_XML);
    if (cliParser.hasOption("conf_file")) {
      Path confFilePath = new Path(cliParser.getOptionValue("conf_file"));

      // if no scheme, assume local file, else read using corresponding filesystem
      if (confFilePath.toUri().getScheme() == null) {
        tonyConf.addResource(confFilePath);
      } else {
        tonyConf.addResource(confFilePath.getFileSystem(hdfsConf).open(confFilePath));
      }
    } else {
      // Search for tony.xml on classpath. Will NOT throw an error if not present on classpath.
      tonyConf.addResource(Constants.TONY_XML);
    }
    if (cliParser.hasOption("conf")) {
      String[] confs = cliParser.getOptionValues("conf");
      for (Map.Entry<String, String> cliConf : Utils.parseKeyValue(confs).entrySet()) {
        String[] existingValue = tonyConf.getStrings(cliConf.getKey());
        if (existingValue != null && TonyConfigurationKeys.MULTI_VALUE_CONF.contains(cliConf.getKey())) {
          ArrayList<String> newValues = new ArrayList<>(Arrays.asList(existingValue));
          newValues.add(cliConf.getValue());
          tonyConf.setStrings(cliConf.getKey(), newValues.toArray(new String[0]));
        } else {
          tonyConf.set(cliConf.getKey(), cliConf.getValue());
        }
      }
    }

    String tonyConfDir = System.getenv(Constants.TONY_CONF_DIR);
    if (tonyConfDir == null) {
      tonyConfDir = Constants.DEFAULT_TONY_CONF_DIR;
    }
    tonyConf.addResource(new Path(tonyConfDir + File.separatorChar + Constants.TONY_SITE_CONF));
  }

  /**
   * This helper method parses and updates tonyConf's jobtype.resources fields
   *  - If the file is remote, keep as is.
   *  - If the file is local and is a file, upload to remote fs and replace this entry with address of uploaded file.
   *  - If the file is local and is a directory, zip the directory, upload to remote fs and replace entry with the
   *  address of the uploaded file.
   **/
  @VisibleForTesting
  public void processTonyConfResources(Configuration tonyConf, FileSystem fs) throws IOException, ParseException {
    Set<String> resourceKeys = tonyConf.getValByRegex(TonyConfigurationKeys.RESOURCES_REGEX).keySet();
    for (String resourceKey : resourceKeys) {
      String[] resources = tonyConf.getStrings(resourceKey);
      if (resources == null) {
        continue;
      }
      for (String resource: resources) {
        LocalizableResource lr = new LocalizableResource(resource, fs);
        // If it is local file, we upload to remote fs first
        if (lr.isLocalFile()) {
          Path localFilePath = lr.getSourceFilePath();
          File file = new File(localFilePath.toString());
          if (!file.exists()) {
            LOG.fatal(localFilePath + " doesn't exist in local filesystem");
            throw new IOException(localFilePath + " doesn't exist in local filesystem.");
          }
          if (file.isFile()) {
            // If it is archive format, set it as ARCHIVE format.
            if (lr.isArchive()) {
              Utils.uploadFileAndSetConfResources(appResourcesPath,
                  localFilePath,
                  lr.getLocalizedFileName(),
                  tonyConf,
                  fs, LocalResourceType.ARCHIVE, resourceKey);
            } else {
              Utils.uploadFileAndSetConfResources(appResourcesPath,
                  localFilePath,
                  lr.getLocalizedFileName(),
                  tonyConf,
                  fs, LocalResourceType.FILE, resourceKey);
            }
          } else {
            // file is directory
            File tmpDir = Files.createTempDir();
            tmpDir.deleteOnExit();
            try {
              java.nio.file.Path dest = Paths.get(tmpDir.getAbsolutePath(), file.getName());
              Utils.zipFolder(Paths.get(resource), dest);
              Utils.uploadFileAndSetConfResources(appResourcesPath,
                  new Path(dest.toString()),
                  lr.getLocalizedFileName(),
                  tonyConf,
                  fs, LocalResourceType.ARCHIVE, resourceKey);
            } finally {
              try {
                FileUtils.deleteDirectory(tmpDir);
              } catch (IOException ex) {
                // ignore the deletion failure and continue
                LOG.warn("Failed to delete temp directory " + tmpDir, ex);
              }
            }
          }
        }
      }
      // Filter out original local file locations
      resources = tonyConf.getStrings(resourceKey);
      resources = Stream.of(resources).filter((filePath) ->
              new Path(filePath).toUri().getScheme() != null
      ).toArray(String[]::new);
      tonyConf.setStrings(resourceKey, resources);
    }

  }

  /**
   * Validates that the configuration does not violate any limits. Throws a {@link RuntimeException}
   * if any limits are exceeded.
   * @param tonyConf  the configuration to validate
   */
  @VisibleForTesting
  static void validateTonyConf(Configuration tonyConf) {
    enforceTaskInstanceLimits(tonyConf);
    enforceResourceLimits(tonyConf);
  }

  /**
   * Enforces that the number of tasks requested does not exceed the max allowed. Throws a {@link RuntimeException}
   * if any limits are exceeded.
   */
  private static void enforceTaskInstanceLimits(Configuration tonyConf) {
    Map<String, TensorFlowContainerRequest> containerRequestMap = Utils.parseContainerRequests(tonyConf);

    // check that we don't request more than the max allowed for any task type
    for (Map.Entry<String, TensorFlowContainerRequest> entry : containerRequestMap.entrySet()) {
      int numInstancesRequested = entry.getValue().getNumInstances();
      int maxAllowedInstances = tonyConf.getInt(TonyConfigurationKeys.getMaxInstancesKey(entry.getKey()), -1);
      if (maxAllowedInstances >= 0 && numInstancesRequested > maxAllowedInstances) {
        throw new RuntimeException("Job requested " + numInstancesRequested + " " + entry.getKey() + " task instances "
            + "but the limit is " + maxAllowedInstances + " " + entry.getKey() + " task instances.");
      }
    }

    // check that we don't request more than the allowed total tasks
    int maxTotalInstances = tonyConf.getInt(TonyConfigurationKeys.MAX_TOTAL_INSTANCES,
        TonyConfigurationKeys.DEFAULT_MAX_TOTAL_INSTANCES);
    int totalRequestedInstances = containerRequestMap.values().stream().mapToInt(TensorFlowContainerRequest::getNumInstances).sum();
    if (maxTotalInstances >= 0 && totalRequestedInstances > maxTotalInstances) {
      throw new RuntimeException("Job requested " + totalRequestedInstances + " total task instances but limit is "
          + maxTotalInstances + ".");
    }
  }

  /**
   * Enforces that the number of resources requested does not exceed the max allowed. Throws a {@link RuntimeException}
   * if any limits are exceeded.
   */
  private static void enforceResourceLimits(Configuration tonyConf) {
    Set<String> jobTypes = Utils.getAllJobTypes(tonyConf);

    // Iterate over all max-total-X resource limits
    for (Map.Entry<String, String> entry
        : tonyConf.getValByRegex(TonyConfigurationKeys.MAX_TOTAL_RESOURCES_REGEX).entrySet()) {
      String maxResourceKey = entry.getKey();
      long maxResourceValue = Long.parseLong(entry.getValue());
      if (maxResourceValue >= 0) {
        Pattern pattern = Pattern.compile(TonyConfigurationKeys.MAX_TOTAL_RESOURCES_REGEX);
        Matcher matcher = pattern.matcher(maxResourceKey);
        if (matcher.matches()) {
          String resource = matcher.group(1);

          // Iterate over all jobtypes and sum up requested X resources.
          // For each jobtype, amount requested is (num X per instance * num instances).
          long totalRequested = 0;
          for (String jobType : jobTypes) {
            int instances = tonyConf.getInt(TonyConfigurationKeys.getInstancesKey(jobType), 0);
            String value = tonyConf.get(TonyConfigurationKeys.getResourceKey(jobType, resource), null);
            if (value != null) {
              long amountPerTask = resource.equals(Constants.MEMORY) ? Long.parseLong(Utils.parseMemoryString(value)) : Long.parseLong(value);
              totalRequested += amountPerTask * instances;
            }
          }

          if (totalRequested > maxResourceValue) {
            throw new RuntimeException("Total amount of " + resource + " (" + totalRequested + ") requested exceeds " + "maximum allowed of "
                + maxResourceValue);
          }
        }
      }
    }
  }

  public Configuration getTonyConf() {
    return this.tonyConf;
  }

  public ContainerLaunchContext createAMContainerSpec(long amMemory, ByteBuffer tokens) throws IOException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    FileSystem fs = FileSystem.get(hdfsConf);
    Map<String, LocalResource> localResources = new HashMap<>();
    addResource(fs, tonyFinalConfPath, LocalResourceType.FILE, Constants.TONY_FINAL_XML, localResources);

    // Add AM resources
    String[] amResources = tonyConf.getStrings(TonyConfigurationKeys.getResourcesKey(Constants.AM_NAME));
    Utils.addResources(amResources, localResources, fs);

    // Add resources for all containers
    amResources = tonyConf.getStrings(TonyConfigurationKeys.getContainerResourcesKey());
    Utils.addResources(amResources, localResources, fs);

    setAMEnvironment(localResources, fs);

    // Set logs to be readable by everyone. Set app to be modifiable only by app owner.
    Map<ApplicationAccessType, String> acls = new HashMap<>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    acls.put(ApplicationAccessType.MODIFY_APP, " ");
    amContainer.setApplicationACLs(acls);

    String command = TonyClient.buildCommand(amMemory);

    LOG.info("Completed setting up Application Master command " + command);
    amContainer.setCommands(ImmutableList.of(command));
    if (tokens != null) {
      amContainer.setTokens(tokens);
    }
    amContainer.setEnvironment(containerEnv);
    amContainer.setLocalResources(localResources);

    return amContainer;
  }

  @VisibleForTesting
  static String buildCommand(long amMemory) {
    List<String> arguments = new ArrayList<>(30);
    arguments.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    arguments.add("-Xmx" + (int) (amMemory * 0.8f) + "m");
    // Add configuration for log dir to retrieve log output from python subprocess in AM
    arguments.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Set class name
    arguments.add("com.linkedin.tony.ApplicationMaster");

    arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDOUT_FILENAME);
    arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDERR_FILENAME);
    return String.join(" ", arguments);
  }

  /**
   * Adds resource to HDFS and local resources map.
   * @param fs HDFS file system reference
   * @param srcPath Source path of resource. If no scheme is included, assumed to be on local filesystem.
   * @param resourceType the type of the src file
   * @param dstPath name of the resource after localization
   * @param localResources the local resources map
   * @throws IOException error when writing to HDFS
   */
  private void addResource(FileSystem fs, String srcPath, LocalResourceType resourceType,
                                 String dstPath, Map<String, LocalResource> localResources) throws IOException {
    Path src = new Path(srcPath);
    Path dst = new Path(appResourcesPath, dstPath);
    HdfsUtils.copySrcToDest(src, dst, hdfsConf);
    fs.setPermission(dst, new FsPermission((short) 0770));
    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            resourceType, LocalResourceVisibility.PRIVATE,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(dstPath, scRsrc);
  }

  private void setAMEnvironment(Map<String, LocalResource> localResources,
                                               FileSystem fs) throws IOException {

    LocalResource tonyConfResource = localResources.get(Constants.TONY_FINAL_XML);
    Utils.addEnvironmentForResource(tonyConfResource, fs, Constants.TONY_CONF_PREFIX, containerEnv);

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : yarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    containerEnv.put("CLASSPATH", classPathEnv.toString());
  }

  // Set up delegation token
  private ByteBuffer getTokens() throws IOException, URISyntaxException, YarnException {
    if (!this.secureMode) {
      return null;
    }
    LOG.info("Running with secure cluster mode. Fetching delegation tokens..");
    Credentials cred = new Credentials();
    String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (fileLocation != null) {
      cred = Credentials.readTokenStorageFile(new File(fileLocation), hdfsConf);
    } else {
      // Tokens have not been pre-written. We need to grab the tokens ourselves.
      LOG.info("Fetching RM delegation token..");
      String tokenRenewer = getRmPrincipal(yarnConf);
      if (tokenRenewer == null) {
        throw new RuntimeException("Failed to get RM principal.");
      }
      final Token<?> rmToken = ConverterUtils.convertFromYarn(yarnClient.getRMDelegationToken(new Text(tokenRenewer)),
                                                     yarnConf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                                                                            YarnConfiguration.DEFAULT_RM_ADDRESS,
                                                                            YarnConfiguration.DEFAULT_RM_PORT));
      cred.addToken(rmToken.getService(), rmToken);
      LOG.info("RM delegation token fetched.");

      String defaultFS = hdfsConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
          CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
      LOG.info("Fetching HDFS delegation token for default namenode: " + defaultFS);
      FileSystem fs = FileSystem.get(hdfsConf);
      Token<?> fsToken = fs.getDelegationToken(tokenRenewer);
      if (fsToken == null) {
        throw new RuntimeException("Failed to get FS delegation token for default FS.");
      }
      cred.addToken(fsToken.getService(), fsToken);
      LOG.info("Default HDFS delegation token fetched.");

      String tonyHistoryLocation = tonyConf.get(TonyConfigurationKeys.TONY_HISTORY_LOCATION);
      if (tonyHistoryLocation != null) {
        fs = new Path(tonyHistoryLocation).getFileSystem(hdfsConf);
        fsToken = fs.getDelegationToken(tokenRenewer);
        if (fsToken == null) {
          throw new RuntimeException("Failed to get FS delegation token for history FS.");
        }
        cred.addToken(fsToken.getService(), fsToken);
        LOG.info("Fetched delegation token for history filesystem HDFS.");
      }

      String[] otherNamenodes = tonyConf.getStrings(TonyConfigurationKeys.OTHER_NAMENODES_TO_ACCESS);
      if (otherNamenodes != null) {
        for (String nnUri : otherNamenodes) {
          String namenodeUri = nnUri.trim();
          LOG.info("Fetching HDFS delegation token for " + nnUri);
          FileSystem otherFS = FileSystem.get(new URI(namenodeUri), hdfsConf);
          final Token<?> otherFSToken = otherFS.getDelegationToken(tokenRenewer);
          if (otherFSToken == null) {
            throw new RuntimeException("Failed to get FS delegation token for configured "
                + "other namenode: " + namenodeUri);
          }
          cred.addToken(otherFSToken.getService(), otherFSToken);
          LOG.info("Fetched HDFS token for " + nnUri);
        }
      }
    }

    LOG.info("Successfully fetched tokens.");
    DataOutputBuffer buffer = new DataOutputBuffer();
    cred.writeTokenStorageToStream(buffer);
    return ByteBuffer.wrap(buffer.getData(), 0, buffer.getLength());
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @return true if application completed successfully
   * @throws YarnException
   * @throws java.io.IOException
   */
  @VisibleForTesting
  public boolean monitorApplication()
      throws YarnException, IOException, InterruptedException {

    boolean isTaskUrlsPrinted = false;
    while (true) {
      // Check app status every 1 second.
      Thread.sleep(1000);

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      YarnApplicationState state = report.getYarnApplicationState();

      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      initRpcClient(report);

      if (amRpcClient != null) {
        Set<TaskInfo> receivedInfos = amRpcClient.getTaskInfos();
        Set<TaskInfo> taskInfoDiff = receivedInfos.stream()
                .filter(taskInfo -> !taskInfos.contains(taskInfo))
                .collect(Collectors.toSet());
        // If task status is changed, invoke callback for all listeners.
        if (!taskInfoDiff.isEmpty()) {
          for (TaskInfo taskInfo : taskInfoDiff) {
            LOG.info("Tasks Status Updated: " + taskInfo);
          }
          for (TaskUpdateListener listener : listeners) {
            listener.onTaskInfosUpdated(receivedInfos);
          }
          taskInfos = receivedInfos;
        }

        // Query AM for taskInfos if taskInfos is empty.
        if (amRpcServerInitialized && !isTaskUrlsPrinted) {
          if (!taskInfos.isEmpty()) {
            // Print TaskUrls
            new TreeSet<>(taskInfos).forEach(task -> Utils.printTaskUrl(task, LOG));
            isTaskUrlsPrinted = true;
          }
        }
      }

      if (YarnApplicationState.FINISHED == state || YarnApplicationState.FAILED == state
          || YarnApplicationState.KILLED == state) {
        LOG.info("Application " + appId.getId() + " finished with YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ", breaking monitoring loop.");
        // Set amRpcClient to null so client does not try to connect to it after completion.
        amRpcClient = null;
        String tonyPortalUrl =
            tonyConf.get(TonyConfigurationKeys.TONY_PORTAL_URL, TonyConfigurationKeys.DEFAULT_TONY_PORTAL_URL);
        Utils.printTonyPortalUrl(tonyPortalUrl, appId.toString(), LOG);
        return FinalApplicationStatus.SUCCEEDED == dsStatus;
      }

      if (appTimeout > 0) {
        if (System.currentTimeMillis() > (clientStartTime + appTimeout)) {
          LOG.info("Reached client specified timeout for application. Killing application"
                   + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
          forceKillApplication();
          return false;
        }
      }
    }
  }

  private void initRpcClient(ApplicationReport report) throws IOException {
    if (!amRpcServerInitialized && report.getRpcPort() != -1) {
      amRpcPort = report.getRpcPort();
      amHost = report.getHost();
      LOG.info("AM host: " + report.getHost());
      LOG.info("AM RPC port: " + report.getRpcPort());

      addClientToAMTokenToUGI(report);
      amRpcClient = ApplicationRpcClient.getInstance(amHost, amRpcPort, yarnConf);
      amRpcServerInitialized = true;
    }
  }

  private void addClientToAMTokenToUGI(ApplicationReport report) throws IOException {
    InetSocketAddress serviceAddr = NetUtils.createSocketAddrForHost(report.getHost(), report.getRpcPort());
    if (UserGroupInformation.isSecurityEnabled()) {
      org.apache.hadoop.yarn.api.records.Token clientToAMToken = report.getClientToAMToken();
      Token<ClientToAMTokenIdentifier> token = ConverterUtils.convertFromYarn(clientToAMToken, serviceAddr);
      UserGroupInformation.getCurrentUser().addToken(token);
    }
  }

  /**
   * Kill a submitted application by sending a call to the RM.
   * @throws YarnException
   * @throws java.io.IOException
   */
  public void forceKillApplication()
      throws YarnException, IOException {
    if (appId != null) {
      yarnClient.killApplication(appId);
    }
  }

  @VisibleForTesting
  public CopyOnWriteArrayList<TaskUpdateListener> getListener() {
    return listeners;
  }

  protected ApplicationRpcClient getAMRpcClient() {
    return this.amRpcClient;
  }

  @Override
  public void close() {
    Utils.cleanupHDFSPath(hdfsConf, appResourcesPath);
  }

  @VisibleForTesting
  public int start() {
    boolean result;
    try {
      result = run();
    } catch (IOException | InterruptedException | URISyntaxException | YarnException | ParseException e) {
      LOG.fatal("Failed to run TonyClient", e);
      result = false;
    }
    if (result) {
      LOG.info("Application completed successfully");
      return 0;
    }
    LOG.error("Application failed to complete successfully");
    return -1;
  }

  /**
   * Look up and return the resource manager's principal. This method
   * automatically does the <code>_HOST</code> replacement in the principal and
   * correctly handles HA resource manager configurations.
   *
   * @param conf the {@link Configuration} file from which to read the
   * principal
   * @return the resource manager's principal string or null if the
   * {@link YarnConfiguration#RM_PRINCIPAL} property is not set in the
   * {@code conf} parameter
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRmPrincipal(Configuration conf) throws IOException {
    String principal = conf.get(YarnConfiguration.RM_PRINCIPAL);
    String prepared = null;

    if (principal != null) {
      prepared = getRmPrincipal(principal, conf);
    }

    return prepared;
  }

  /**
   * Perform the <code>_HOST</code> replacement in the {@code principal},
   * Returning the result. Correctly handles HA resource manager configurations.
   *
   * @param rmPrincipal the principal string to prepare
   * @param conf the configuration
   * @return the prepared principal string
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRmPrincipal(String rmPrincipal, Configuration conf)
      throws IOException {
    if (rmPrincipal == null) {
      throw new IllegalArgumentException("RM principal string is null");
    }

    if (HAUtil.isHAEnabled(conf)) {
      conf = getYarnConfWithRmHaId(conf);
    }

    String hostname = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT).getHostName();

    return SecurityUtil.getServerPrincipal(rmPrincipal, hostname);
  }

  /**
   * Returns a {@link YarnConfiguration} built from the {@code conf} parameter
   * that is guaranteed to have the {@link YarnConfiguration#RM_HA_ID}
   * property set.
   *
   * @param conf the base configuration
   * @return a {@link YarnConfiguration} built from the base
   * {@link Configuration}
   * @throws IOException thrown if the {@code conf} parameter contains
   * inconsistent properties
   */
  @VisibleForTesting
  static YarnConfiguration getYarnConfWithRmHaId(Configuration conf)
      throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);

    if (yarnConf.get(YarnConfiguration.RM_HA_ID) == null) {
      // If RM_HA_ID is not configured, use the first of RM_HA_IDS.
      // Any valid RM HA ID should work.
      String[] rmIds = yarnConf.getStrings(YarnConfiguration.RM_HA_IDS);

      if ((rmIds != null) && (rmIds.length > 0)) {
        yarnConf.set(YarnConfiguration.RM_HA_ID, rmIds[0]);
      } else {
        throw new IOException("RM_HA_IDS property is not set for HA resource "
            + "manager");
      }
    }

    return yarnConf;
  }

  public void addListener(TaskUpdateListener listener) {
    listeners.add(listener);
  }

  public void removeListener(TaskUpdateListener listener) {
    listeners.remove(listener);
  }

  public static void main(String[] args) {
    int exitCode = 0;

    // Adds hadoop-inject.xml as a default resource so Azkaban metadata will be present in the new Configuration created
    HadoopConfigurationInjector.injectResources(new Props() /* ignored */);
    try (TonyClient client = new TonyClient(new Configuration())) {
      boolean sanityCheck = client.init(args);
      if (!sanityCheck) {
        LOG.fatal("Failed to init client.");
        exitCode = -1;
      }

      if (exitCode == 0) {
        exitCode = client.start();
        if (client.amRpcClient != null) {
          client.amRpcClient.finishApplication();
          LOG.info("Sent message to AM to stop.");
        }
      }
    } catch (ParseException | IOException | YarnException e) {
      LOG.fatal("Encountered exception while initializing client or finishing application.", e);
      exitCode = -1;
    }
    System.exit(exitCode);
  }

}
