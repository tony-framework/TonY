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
import com.linkedin.tony.rpc.impl.TaskStatus;
import com.linkedin.tony.security.TokenCache;
import com.linkedin.tony.models.JobContainerRequest;
import com.linkedin.tony.util.HdfsUtils;
import com.linkedin.tony.util.Utils;
import com.linkedin.tony.util.VersionInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.api.records.ContainerReport;
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

import static com.linkedin.tony.Constants.SIDECAR_TB_LOG_DIR;
import static com.linkedin.tony.Constants.SIDECAR_TB_ROLE_NAME;
import static com.linkedin.tony.Constants.SIDECAR_TB_SCIRPT_FILE_NAME;
import static com.linkedin.tony.Constants.SIDECAR_TB_TEST_KEY;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_GPUS;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_INSTANCES;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_MEMORY;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_TB_VCORE;
import static com.linkedin.tony.TonyConfigurationKeys.SIDECAR_JOBTYPES;
import static com.linkedin.tony.TonyConfigurationKeys.TB_GPUS;
import static com.linkedin.tony.TonyConfigurationKeys.TB_INSTANCES;
import static com.linkedin.tony.TonyConfigurationKeys.TB_MEMORY;
import static com.linkedin.tony.TonyConfigurationKeys.TB_VCORE;
import static com.linkedin.tony.TonyConfigurationKeys.TENSORBOARD_LOG_DIR;


/**
 * User entry point to submit tensorflow job.
 */
public class TonyClient implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(TonyClient.class);

  // Configurations
  private YarnClient yarnClient;
  private HdfsConfiguration hdfsConf = new HdfsConfiguration();
  private YarnConfiguration yarnConf = new YarnConfiguration();
  private Configuration mapredConf = new Configuration();
  private Options opts;

  // RPC
  private String amHost;
  private int amRpcPort;
  private boolean amRpcServerInitialized = false;
  private ApplicationRpcClient amRpcClient;

  // Containers set up.
  private String hdfsConfAddress = null;
  private String yarnConfAddress = null;
  private String mapredConfAddress = null;
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
  private Map<String, String> containerEnv = new HashMap<>();
  private String hadoopFrameworkLocation = null;
  private String hadoopFrameworkClasspath = null;
  private String sidecarTBScriptPath = null;

  private String tonyFinalConfPath;
  private String tonySrcZipPath;
  private Configuration tonyConf;
  private final long clientStartTime = System.currentTimeMillis();
  private ApplicationId appId;
  private Path appResourcesPath;
  private int hbInterval;
  private int maxHbMisses;

  private boolean debug = false;

  private CallbackHandler callbackHandler;
  private CopyOnWriteArrayList<TaskUpdateListener> listeners = new CopyOnWriteArrayList<>();

  // For access from CLI.
  private Set<TaskInfo> taskInfos = new HashSet<>();

  /**
   * Gets default hadoop application classpath from yarnConf.
   */
  public static String getDefaultHadoopClasspath(Configuration yarnConf) {
    StringBuilder classPathBuilder = new StringBuilder();
    for (String c : yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      if (classPathBuilder.length() > 0) {
        classPathBuilder.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      }
      classPathBuilder.append(c.trim());
    }
    return classPathBuilder.toString();
  }

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
    String tonySrcZipName = Utils.getTonySrcZipName(appId.toString());
    if (srcDir != null) {
      if (Utils.isArchive(srcDir)) {
        Utils.uploadFileAndSetConfResources(appResourcesPath, new Path(srcDir),
            tonySrcZipName, tonyConf, fs, LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
      } else {
        Utils.zipFolder(Paths.get(srcDir), Paths.get(tonySrcZipName));
        Utils.uploadFileAndSetConfResources(appResourcesPath, new Path(tonySrcZipName),
            tonySrcZipName, tonyConf, fs, LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
      }
    }
    this.tonySrcZipPath = tonySrcZipName;

    if (pythonVenv != null) {
      Utils.uploadFileAndSetConfResources(appResourcesPath,
          new Path(pythonVenv), Constants.PYTHON_VENV_ZIP, tonyConf, fs, LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
    }

    if (sidecarTBScriptPath != null) {
      Utils.uploadFileAndSetConfResources(appResourcesPath,
              new Path(sidecarTBScriptPath), SIDECAR_TB_SCIRPT_FILE_NAME, tonyConf, fs, LocalResourceType.FILE,
              TonyConfigurationKeys.getContainerResourcesKey());
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

    // Update map reduce framework configuration so that the AM won't need to resolve it again.
    if (!hadoopFrameworkClasspath.isEmpty()) {
      tonyConf.set(TonyConfigurationKeys.APPLICATION_HADOOP_CLASSPATH, hadoopFrameworkClasspath);
    }
    if (!hadoopFrameworkLocation.isEmpty()) {
      try {
        // hadoopFrameworkLocation format: [scheme]://[host][path]#[fragment].
        // For example, hdfs://ltx1-1234/mapred/framework/hadoop-mapreduce-3.1.2.tar#mrframework
        URI hadoopFrameworkURI = new URI(hadoopFrameworkLocation);

        // If fragment was defined in the URI, it is used as the alias of the localized file. For example,
        // hdfs://ltx1-1234/mapred/framework/hadoop-mapreduce-3.1.2.tar#mrframework will be localized as
        // mrframework rather than hadoop-mapreduce-3.1.2.tar in the container.
        if (hadoopFrameworkURI.getFragment() != null) {
          String localizedFileName = hadoopFrameworkURI.getFragment();
          // Remove fragment in the mapReduceFrameworkURI so that it can be located on file system.
          URI uriWithoutFragment = new URI(
              hadoopFrameworkURI.getScheme(),
              hadoopFrameworkURI.getSchemeSpecificPart(),
              null);
          Utils.appendConfResources(
              TonyConfigurationKeys.getContainerResourcesKey(),
              // hadoop framework location contains an archive. Rename archive name to localizedFileName.
              uriWithoutFragment + Constants.RESOURCE_DIVIDER + localizedFileName + Constants.ARCHIVE_SUFFIX,
              tonyConf);
        } else {
          Utils.appendConfResources(
              TonyConfigurationKeys.getContainerResourcesKey(),
              hadoopFrameworkURI + Constants.ARCHIVE_SUFFIX,
              tonyConf);
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(
            "Failed to parse Hadoop framework Location " + hadoopFrameworkLocation + " to URI.", e);
      }
    }

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
      throws YarnException, IOException {

    String appName = tonyConf.get(TonyConfigurationKeys.APPLICATION_NAME,
        TonyConfigurationKeys.DEFAULT_APPLICATION_NAME);
    appContext.setApplicationName(appName);
    String appType = tonyConf.get(TonyConfigurationKeys.APPLICATION_TYPE,
        TonyConfigurationKeys.DEFAULT_APPLICATION_TYPE);
    appContext.setApplicationType(appType);
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

  private void initMapRedConf() {
    String hadoopConfDir = System.getenv(Constants.HADOOP_CONF_DIR);
    if (hadoopConfDir != null) {
      mapredConf.addResource(new Path(hadoopConfDir + File.separatorChar + Constants.CORE_SITE_CONF));
      mapredConf.addResource(new Path(hadoopConfDir + File.separatorChar + Constants.MAPRED_SITE_CONF));
    }

    if (mapredConfAddress != null) {
      mapredConf.addResource(new Path(mapredConfAddress));
    }
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
    opts.addOption("sidecar_tensorboard_log_dir", true, "Enable sidecar tensorboard");
    opts.addOption("help", false, "Print usage");
    opts.addOption("debug", false, "Reserve tony_src_application_xxxx.zip and tony_src_xxx.zip for debug");
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
    mapredConfAddress = tonyConf.get(TonyConfigurationKeys.MAPRED_CONF_LOCATION);
    initHdfsConf();
    createYarnClient();
    initMapRedConf();

    hadoopFrameworkLocation = tonyConf.get(
        TonyConfigurationKeys.APPLICATION_HADOOP_LOCATION,
        mapredConf.get(Constants.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, ""));
    hadoopFrameworkClasspath = tonyConf.get(
        TonyConfigurationKeys.APPLICATION_HADOOP_CLASSPATH,
        mapredConf.get(Constants.MAPREDUCE_APPLICATION_CLASSPATH, ""));
    // Classpath in hadoopFrameworkClasspath was separated by comma.
    // Replace it with ApplicationConstants.CLASS_PATH_SEPARATOR.
    if (!hadoopFrameworkClasspath.isEmpty()) {
      hadoopFrameworkClasspath = hadoopFrameworkClasspath.replace(
          ",", ApplicationConstants.CLASS_PATH_SEPARATOR);
    }

    taskParams = cliParser.getOptionValue("task_params");
    pythonBinaryPath = cliParser.getOptionValue("python_binary_path");
    pythonVenv = cliParser.getOptionValue("python_venv");
    executes = cliParser.getOptionValue("executes");
    executes = TonyClient.buildTaskCommand(pythonVenv, pythonBinaryPath, executes, taskParams);
    if (executes != null) {
      tonyConf.set(TonyConfigurationKeys.getContainerExecuteCommandKey(), executes);
      String pythonExecPath = getPythonInterpreter(pythonVenv, pythonBinaryPath);
      if (null != pythonExecPath) {
        tonyConf.set(TonyConfigurationKeys.PYTHON_EXEC_PATH, pythonExecPath);
      }
    }

    // src_dir & hdfs_classpath flags are for compatibility.
    srcDir = cliParser.getOptionValue("src_dir");

    applicationTags = new HashSet<>(
        tonyConf.getStringCollection(TonyConfigurationKeys.APPLICATION_TAGS));

    // Set hdfsClassPath for all workers
    // Prepend hdfs:// if missing
    String allHdfsClasspathsString = cliParser.getOptionValue("hdfs_classpath");
    hdfsClasspath = parseHdfsClasspaths(allHdfsClasspathsString);

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                                         + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                                         + " Specified virtual cores=" + amVCores);
    }

    if (Utils.getNumTotalTasks(tonyConf) == 0 && amGpus > 0) {
      LOG.warn("It seems you reserved " + amGpus + " GPUs in application master (driver, which doesn't perform "
          + "training) during distributed training.");
    }

    appTimeout = tonyConf.getInt(TonyConfigurationKeys.APPLICATION_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_APPLICATION_TIMEOUT);

    List<String> executionEnvPair = new ArrayList<>();
    if (tonyConf.get(TonyConfigurationKeys.EXECUTION_ENV) != null) {
      String[] envs = tonyConf.getStrings(TonyConfigurationKeys.EXECUTION_ENV);
      executionEnvPair.addAll(Arrays.asList(envs));
    }
    if (cliParser.hasOption("shell_env")) {
      String[] envs = cliParser.getOptionValues("shell_env");
      executionEnvPair.addAll(Arrays.asList(envs));
    }

    Map<String, String> dockerEnv = HadoopCompatibleAdapter.getContainerEnvForDocker(tonyConf, Constants.AM_NAME);
    containerEnv.putAll(dockerEnv);

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

    if (cliParser.hasOption("sidecar_tensorboard_log_dir") || tonyConf.get(TENSORBOARD_LOG_DIR) != null) {
      String tbLogDir = cliParser.getOptionValue("sidecar_tensorboard_log_dir");
      setSidecarTBResources(tbLogDir, executionEnvPair);
    }

    if (!executionEnvPair.isEmpty()) {
      tonyConf.setStrings(TonyConfigurationKeys.EXECUTION_ENV, executionEnvPair.toArray(new String[0]));
    }

    if (cliParser.hasOption("debug")) {
      this.debug = true;
    }

    return true;
  }

  private void setSidecarTBResources(String tbLogDir, List<String> executionEnvPair) {
    tonyConf.set(TENSORBOARD_LOG_DIR, tbLogDir);

    tonyConf.set(TB_INSTANCES, String.valueOf(DEFAULT_TB_INSTANCES));
    tonyConf.set(TB_VCORE, tonyConf.get(TB_VCORE, String.valueOf(DEFAULT_TB_VCORE)));
    tonyConf.set(TB_MEMORY, tonyConf.get(TB_MEMORY, DEFAULT_TB_MEMORY));
    tonyConf.set(TB_GPUS, tonyConf.get(TB_GPUS, String.valueOf(DEFAULT_TB_GPUS)));

    List<String> sidecarTypes = new ArrayList<>(Arrays.asList(Utils.getSidecarJobTypes(tonyConf)));
    sidecarTypes.add(SIDECAR_TB_ROLE_NAME);
    tonyConf.set(SIDECAR_JOBTYPES, StringUtils.join(sidecarTypes, ","));

    String pythonInterpreter = "python";
    if (pythonBinaryPath != null) {
      pythonInterpreter = getPythonInterpreter(pythonVenv, pythonBinaryPath);
    }

    String scriptName = SIDECAR_TB_SCIRPT_FILE_NAME;
    sidecarTBScriptPath = getFilePathFromResource(scriptName);
    String tbCommandKey = "tony." + SIDECAR_TB_ROLE_NAME + ".command";
    if (tonyConf.get(tbCommandKey) == null) {
      String startupTBCommand = String.format("%s %s", pythonInterpreter, scriptName);
      tonyConf.set(tbCommandKey, startupTBCommand);
    }

    executionEnvPair.add(String.format("%s=%s", SIDECAR_TB_LOG_DIR, tbLogDir));
    if (System.getenv(SIDECAR_TB_TEST_KEY) != null) {
      executionEnvPair.add(String.format("%s=%s", SIDECAR_TB_TEST_KEY, "true"));
    }
  }

  static String getFilePathFromResource(String fileName) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try {
      java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory(fileName);
      tempDir.toFile().deleteOnExit();
      try (InputStream stream = loader.getResourceAsStream(fileName)) {
        java.nio.file.Files.copy(stream, Paths.get(tempDir.toAbsolutePath().toString(), fileName));
      }
      return Paths.get(tempDir.toAbsolutePath().toString(), fileName).toFile().getAbsolutePath();
    } catch (Exception e) {
      LOG.info(e);
      return null;
    }
  }

  @VisibleForTesting
  static String buildTaskCommand(String pythonVenv, String pythonBinaryPath, String execute,
                                 String taskParams) {
    if (execute == null) {
      return null;
    }
    String baseTaskCommand = execute;

    if (pythonBinaryPath != null) {
      String pythonInterpreter = getPythonInterpreter(pythonVenv, pythonBinaryPath);
      baseTaskCommand = pythonInterpreter + " " + execute;
    }

    if (taskParams != null) {
      baseTaskCommand += " " + taskParams;
    }

    return baseTaskCommand;
  }

  private static String getPythonInterpreter(String pythonVenv, String pythonBinaryPath) {
    if (pythonBinaryPath == null) {
      return null;
    }

    String pythonInterpreter;
    if (pythonBinaryPath.startsWith("/") || pythonVenv == null) {
      pythonInterpreter = pythonBinaryPath;
    } else {
      pythonInterpreter = Constants.PYTHON_VENV_DIR + File.separatorChar  + pythonBinaryPath;
    }

    return pythonInterpreter;
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
      Set<String> resourcesToBeRemoved = new HashSet<>();
      for (String resource: resources) {
        // If a hdfs classpath does not exist, we skip it rather than failing the job.
        // This is because there are some cases where while constructing a ML flow, we have a hdfs classpath that might
        // or might not exist depending on run time behavior. So we specify both paths in that case and disregard the
        // non-existent once
        LocalizableResource lr;
        try {
          lr = new LocalizableResource(resource, fs);
        } catch (FileNotFoundException ex) {
          if (hdfsClasspath.contains(resource)) {
            LOG.warn("HDFS classpath does not exist for: " + resource);
            resourcesToBeRemoved.add(resource);
            continue;
          } else {
            throw ex;
          }
        }
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
              new Path(filePath).toUri().getScheme() != null && !resourcesToBeRemoved.contains(filePath)
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
    Map<String, JobContainerRequest> containerRequestMap = Utils.parseContainerRequests(tonyConf);

    // check that we don't request more than the max allowed for any task type
    for (Map.Entry<String, JobContainerRequest> entry : containerRequestMap.entrySet()) {
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
    int totalRequestedInstances = containerRequestMap.values().stream().mapToInt(JobContainerRequest::getNumInstances).sum();
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

    String command = buildCommand(amMemory);

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
  String buildCommand(long amMemory) {
    List<String> arguments = new ArrayList<>(30);
    arguments.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    arguments.add("-Xmx" + (int) (amMemory * 0.8f) + "m");
    // Add configuration for log dir to retrieve log output from python subprocess in AM
    arguments.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Add am jvm configuration
    String amJvm = tonyConf.get(TonyConfigurationKeys.TASK_AM_JVM_OPTS);
    if (org.apache.commons.lang3.StringUtils.isNotBlank(amJvm)) {
      arguments.add(amJvm);
    }
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

    String hadoopFrameworkClasspath = this.hadoopFrameworkClasspath;
    if (hadoopFrameworkClasspath.isEmpty()) {
      // Get standard hadoop classpath from Yarn configuration.
      hadoopFrameworkClasspath = getDefaultHadoopClasspath(yarnConf);
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
    classPathEnv.append(hadoopFrameworkClasspath);

    containerEnv.put("CLASSPATH", classPathEnv.toString());
  }

  // Set up delegation token
  private ByteBuffer getTokens() throws IOException, YarnException {
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

      // Fetching HDFS tokens
      LOG.info("Fetching HDFS delegation tokens for default, history and other namenodes...");
      List<Path> pathList = new ArrayList<>();
      pathList.add(appResourcesPath);

      String tonyHistoryLocation = tonyConf.get(TonyConfigurationKeys.TONY_HISTORY_LOCATION);
      if (tonyHistoryLocation != null) {
        pathList.add(new Path(tonyHistoryLocation));
      }

      String[] otherNamenodes = tonyConf.getStrings(TonyConfigurationKeys.OTHER_NAMENODES_TO_ACCESS);
      if (otherNamenodes != null) {
        for (String nnUri : otherNamenodes) {
          pathList.add(new Path(nnUri.trim()));
        }
      }

      Path[] paths = pathList.toArray(new Path[0]);
      TokenCache.obtainTokensForNamenodes(cred, paths, hdfsConf, tokenRenewer);
      LOG.info("Fetched HDFS delegation token.");
    }

    LOG.info("Successfully fetched tokens.");
    DataOutputBuffer buffer = new DataOutputBuffer();
    cred.writeTokenStorageToStream(buffer);
    return ByteBuffer.wrap(buffer.getData(), 0, buffer.getLength());
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @return true if application completed successfully and false otherwise
   * @throws YarnException
   * @throws java.io.IOException
   */
  @VisibleForTesting
  public boolean monitorApplication() throws YarnException, IOException, InterruptedException {
    boolean result;
    while (true) {
      // Check app status every 1 second.
      Thread.sleep(1000);

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      YarnApplicationState appState = report.getYarnApplicationState();

      FinalApplicationStatus finalApplicationStatus = report.getFinalApplicationStatus();
      initRpcClientAndLogAMUrl(report);

      boolean isFirstTimePrint = taskInfos.isEmpty();
      boolean isTaskUpdated = updateTaskInfoAndReturn();
      if (isTaskUpdated) {
        if (isFirstTimePrint) {
          // if it's first time printing tasks, log detailed info of the tasks, including
          // name, index, status, and URL.
          LOG.info("------  Application (re)starts, status of ALL tasks ------");
          logTaskInfo(taskInfos);
        } else {
          // if tasks are already logged before, then only print task name, index, and status. NOT
          // print URL which could overwhelm the log. Users can always find the URL of corresponding
          // task they are interested from previously printed logs.
          LOG.info("------ Task Status Updated ------");
          logSimplifiedTaskInfo(taskInfos);
        }
      }

      if (YarnApplicationState.KILLED == appState) {
        LOG.warn("Application " + appId.getId() + " was killed. YarnState: " + appState + ". "
            + "FinalApplicationStatus = " + finalApplicationStatus + ".");
        // Set amRpcClient to null so client does not try to connect to a killed AM.
        amRpcClient = null;
        result = false;
        break;
      }

      if (YarnApplicationState.FINISHED == appState || YarnApplicationState.FAILED == appState) {
        updateTaskInfoAndReturn();
        LOG.info("-----  Application finished, status of ALL tasks -----");
        // log detailed task info including URL so that users can check the URL of failed worker
        // quickly without the need to scroll up to the top to find out the URL.
        logTaskInfo(taskInfos);
        LOG.info("Application " + appId.getId() + " finished with YarnState=" + appState
            + ", DSFinalStatus=" + finalApplicationStatus + ", breaking monitoring loop.");
        String tonyPortalUrl =
            tonyConf.get(TonyConfigurationKeys.TONY_PORTAL_URL, TonyConfigurationKeys.DEFAULT_TONY_PORTAL_URL);
        Utils.printTonyPortalUrl(tonyPortalUrl, appId.toString(), LOG);
        result = FinalApplicationStatus.SUCCEEDED == finalApplicationStatus;
        break;
      }

      if (appTimeout > 0) {
        if (System.currentTimeMillis() > (clientStartTime + appTimeout)) {
          LOG.info("Reached client specified timeout for application. Killing application"
                   + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
          forceKillApplication();
          result = false;
          break;
        }
      }
    }

    signalAMToFinish();
    return result;
  }

  private void signalAMToFinish() {
    if (amRpcClient != null) {
      LOG.info("Sending message to AM to stop.");
      try {
        amRpcClient.finishApplication();
      } catch (Exception e) {
        LOG.error("Errors on calling AM to finish application. Maybe AM has finished.", e);
      }
      amRpcClient = null;
    }
  }

  /**
   * Logs task info. Task info will be sorted by status, then name, then index in the log.
   * Task order follows {@link TaskStatus#STATUS_SORTED_BY_ATTENTION}. For tasks of same status,
   * sorts them based on name, then index.
   * An example of output:
   *   FAILED, chief, 0, some url
   *   FAILED, ps, 0, some url
   *   FINISHED, worker, 0, some url
   *   FINISHED, worker, 1, some url
   */
  private static void logTaskInfo(Collection<TaskInfo> tasks) {
    List<TaskInfo> sortedTasks = new ArrayList<>();
    sortedTasks.addAll(tasks);
    Collections.sort(sortedTasks);
    String log = "%s, %s, %s, %s";
    for (TaskInfo taskInfo : sortedTasks) {
      LOG.info(String.format(log, taskInfo.getStatus(), taskInfo.getName(),
          taskInfo.getIndex(), taskInfo.getUrl()));
    }
  }

  /**
   * Merge tasks by task names.
   * E.g:
   * input is a list of TaskInfo:
   *     TaskInfo("ps", "0", "url")
   *     TaskInfo("ps", "1", "url")
   *     TaskInfo("worker", "0", "url")
   *     TaskInfo("worker", "1", "url")
   * returns:
   *     [TaskInfo] name: ps {0, 1}, worker {0, 1}
   *
   * @param tasks
   */
  @VisibleForTesting
  static String mergeTasks(List<TaskInfo> tasks) {
    StringBuffer toBePrinted = new StringBuffer();
    List<String> taskGroup = new ArrayList<>();
    for (int i = 0; i < tasks.size(); i++) {
      taskGroup.add(tasks.get(i).getIndex());
      if (i == tasks.size() - 1 || !tasks.get(i).getName().equals(tasks.get(i + 1).getName())) {
        toBePrinted.append(tasks.get(i).getName() + " [" + StringUtils.join(taskGroup,
            ", ") + "]" + " ");
        taskGroup.clear();
      }
    }
    return toBePrinted.toString();
  }

  /**
   * Logs task name, index and status. Intentionally skip logging task URL to avoid info
   * overwhelming.
   * Log looks like following (group by status, and then sorted the tasks by task name then index):
   * Status FAILED: ps {0, 1, 2} worker {1}
   * Status RUNNING: ps {3} worker {0}
   * @param tasks
   */
  private static void logSimplifiedTaskInfo(Collection<TaskInfo> tasks) {
    List<TaskInfo> sortedTasks = new ArrayList<>();
    sortedTasks.addAll(tasks);
    Collections.sort(sortedTasks);

    for (TaskStatus status : TaskStatus.STATUS_SORTED_BY_ATTENTION) {
      // Get the tasks with the status
      List<TaskInfo> taskInfoPerStatus =
          sortedTasks.stream().filter(c -> c.getStatus().equals(status)).collect(Collectors.toList());
      if (!taskInfoPerStatus.isEmpty()) {
        LOG.info(status + ": " + mergeTasks(taskInfoPerStatus));
      }
    }
  }

  /**
   * Updates task info and returns whether any task is updated.
   */
  private boolean updateTaskInfoAndReturn() {
    boolean taskUpdated = false;
    if (amRpcClient != null) {
      try {
        Set<TaskInfo> receivedInfos = amRpcClient.getTaskInfos();
        taskUpdated = !taskInfos.equals(receivedInfos);
        // If task status is changed, invoke callback for all listeners.
        if (taskUpdated) {
          for (TaskUpdateListener listener : listeners) {
            listener.onTaskInfosUpdated(receivedInfos);
          }
          taskInfos = receivedInfos;
        }
      } catch (IOException | YarnException e) {
        LOG.error("Errors on calling AM to update task infos.", e);
      }
    }
    return taskUpdated;
  }

  private void initRpcClientAndLogAMUrl(ApplicationReport report) throws IOException {
    if (!amRpcServerInitialized && report.getRpcPort() != -1) {
      try {
        ContainerReport amContainerReport = yarnClient.getContainers(report.getCurrentApplicationAttemptId())
            .stream()
            .min(Comparator.comparingLong(x -> x.getContainerId().getContainerId()))
            .orElseThrow(YarnException::new);
        LOG.info("Driver (application master) log url: " + amContainerReport.getLogUrl());
      } catch (YarnException | IOException e) {
        LOG.warn("Failed to get containers for attemptId: " + report.getCurrentApplicationAttemptId(), e);
      }

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
   * Parse HDFS class paths by prepending hdfs:// if missing and adding to conf resources.
   * @param rawHdfsClasspaths Comma separated hdfs class paths.
   * @return Comma separated hdfs classpaths that have been parsed and prepended with hdfs://.
   */
  private String parseHdfsClasspaths(String rawHdfsClasspaths) throws IOException {
    if (rawHdfsClasspaths == null) {
      return null;
    }
    // rawHdfsClasspaths may contain multiple classpaths that are comma separated. We need to prepend
    // hdfs:// to all paths if missing.
    String[] allHdfsClasspaths = rawHdfsClasspaths.split(",");
    for (int i = 0; i < allHdfsClasspaths.length; i++) {
      String validPath = allHdfsClasspaths[i];
      if (validPath != null && !validPath.startsWith(FileSystem.get(hdfsConf).getScheme())) {
        validPath = FileSystem.getDefaultUri(hdfsConf) + validPath;
      }
      Utils.appendConfResources(TonyConfigurationKeys.getContainerResourcesKey(), validPath, tonyConf);
      allHdfsClasspaths[i] = validPath;
    }
    return String.join(",", allHdfsClasspaths);
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
    } finally {
      if (!this.debug) {
        cleanupLocalTmpFiles();
      }
    }
    if (result) {
      LOG.info("Application completed successfully");
      return 0;
    }
    LOG.error("Application failed to complete successfully");
    return -1;
  }

  private void cleanupLocalTmpFiles() {
    if (this.tonySrcZipPath != null) {
      Utils.cleanupLocalFile(this.tonySrcZipPath);
    }
    if (this.tonyFinalConfPath != null) {
      Utils.cleanupLocalFile(this.tonyFinalConfPath);
    }
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
    int exitCode;

    // Adds hadoop-inject.xml as a default resource so Azkaban metadata will be present in the new Configuration created
    HadoopConfigurationInjector.injectResources(new Props() /* ignored */);
    try (TonyClient client = new TonyClient(new Configuration())) {
      boolean sanityCheck = client.init(args);
      if (!sanityCheck) {
        LOG.fatal("Failed to init client.");
        exitCode = -1;
      } else {
        exitCode = client.start();
      }
    } catch (ParseException | IOException e) {
      LOG.fatal("Encountered exception while initializing client or finishing application.", e);
      exitCode = -1;
    }
    System.exit(exitCode);
  }

}
