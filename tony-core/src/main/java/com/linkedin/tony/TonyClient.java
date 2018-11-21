/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.tony.rpc.TaskUrl;
import com.linkedin.tony.rpc.impl.ApplicationRpcClient;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
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
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import static com.linkedin.tony.Constants.*;


/**
 * User entry point to submit tensorflow job.
 */
public class TonyClient implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(TonyClient.class);

  private static final String APP_TYPE = "TENSORFLOW";
  private static final String ARCHIVE_SUFFIX = "tony_archive.zip";
  private static final String TONY_CONF_DIR = Constants.TONY_CONF_DIR;
  private static final String TONY_SITE_CONF = Constants.TONY_SITE_CONF;
  private static final String TONY_DEFAULT_XML = Constants.TONY_DEFAULT_XML;
  private static final String TONY_XML = Constants.TONY_XML;

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
  private String pythonVenv = "";
  private String executes;
  private String hdfsClasspath;
  private String srcDir;
  private long appTimeout;
  private boolean secureMode;
  private Map<String, String> shellEnv = new HashMap<>();
  private Map<String, String> containerEnv = new HashMap<>();

  private String archivePath;
  private String tonyFinalConfPath;
  private Configuration tonyConf;
  private final long clientStartTime = System.currentTimeMillis();
  private Path appResourcesPath;
  private int hbInterval;
  private int maxHbMisses;

  // For access from CLI.
  private Set<TaskUrl> taskUrls = new HashSet<>();

  public TonyClient() {
    this(new Configuration(false));
  }

  public TonyClient(Configuration conf) {
    initOptions();
    tonyConf = conf;
  }

  public ImmutableSet<TaskUrl> getTaskUrls() {
    return ImmutableSet.copyOf(taskUrls);
  }

  private boolean run() throws IOException, InterruptedException, URISyntaxException, YarnException {
    LOG.info("Starting client..");
    yarnClient.start();
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    long maxMem = appResponse.getMaximumResourceCapability().getMemorySize();

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

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    this.archivePath = Utils.getClientResourcesPath(appId.toString(), ARCHIVE_SUFFIX);
    zipArchive();

    this.tonyFinalConfPath = Utils.getClientResourcesPath(appId.toString(), Constants.TONY_FINAL_XML);
    // Write user's overridden conf to an xml to be localized.
    try (OutputStream os = new FileOutputStream(this.tonyFinalConfPath)) {
      tonyConf.writeXml(os);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create " + this.tonyFinalConfPath + " conf file. Exiting.", e);
    }

    String appName = tonyConf.get(TonyConfigurationKeys.APPLICATION_NAME,
        TonyConfigurationKeys.DEFAULT_APPLICATION_NAME);
    appContext.setApplicationName(appName);
    appContext.setApplicationType(APP_TYPE);

    // Set up resource type requirements
    Resource capability = Resource.newInstance(amMemory, amVCores);
    Utils.setCapabilityGPU(capability, amGpus);
    appContext.setResource(capability);

    // Set the queue to which this application is to be submitted in the RM
    String yarnQueue = tonyConf.get(TonyConfigurationKeys.YARN_QUEUE_NAME,
        TonyConfigurationKeys.DEFAULT_YARN_QUEUE_NAME);
    appContext.setQueue(yarnQueue);

    // Set the ContainerLaunchContext to describe the Container ith which the TonyApplicationMaster is launched.
    ContainerLaunchContext amSpec =
        createAMContainerSpec(appId, this.amMemory, this.taskParams, this.pythonBinaryPath,
                              this.pythonVenv, this.executes, getTokens());
    appContext.setAMContainerSpec(amSpec);
    String nodeLabel = tonyConf.get(TonyConfigurationKeys.APPLICATION_NODE_LABEL);
    if (nodeLabel != null) {
      appContext.setNodeLabelExpression(nodeLabel);
    }
    LOG.info("Submitting YARN application");
    yarnClient.submitApplication(appContext);
    ApplicationReport report = yarnClient.getApplicationReport(appId);
    logTrackingAndRMUrls(report);
    return monitorApplication(appId);
  }

  private void logTrackingAndRMUrls(ApplicationReport report) {
    LOG.info("URL to track running application (will proxy to TensorBoard once it has started): "
             + report.getTrackingUrl());
    LOG.info("ResourceManager web address for application: "
        + Utils.buildRMUrl(yarnConf, report.getApplicationId().toString()));
  }

  private void createYarnClient() {
    if (this.yarnConfAddress != null) {
      this.yarnConf.addResource(new Path(this.yarnConfAddress));
    }
    if (this.hdfsConfAddress != null) {
      this.hdfsConf.addResource(new Path(this.hdfsConfAddress));
    }
    int numRMConnectRetries = tonyConf.getInt(TonyConfigurationKeys.RM_CLIENT_CONNECT_RETRY_MULTIPLIER,
        TonyConfigurationKeys.DEFAULT_RM_CLIENT_CONNECT_RETRY_MULTIPLIER);
    long rmMaxWaitMS = yarnConf.getLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS) * numRMConnectRetries;
    yarnConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, rmMaxWaitMS);

    if (System.getenv(Constants.HADOOP_CONF_DIR) != null) {
      hdfsConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.CORE_SITE_CONF));
      yarnConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.CORE_SITE_CONF));
      hdfsConf.addResource(new Path(System.getenv(Constants.HADOOP_CONF_DIR) + File.separatorChar + Constants.HDFS_SITE_CONF));
    }
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
  }

  private void initOptions() {
    opts = Utils.getCommonOptions();
    opts.addOption("conf", true, "User specified configuration, as key=val pairs");
    opts.addOption("conf_file", true, "Name of user specified conf file, on the classpath");
    opts.addOption("src_dir", true, "Name of directory of source files.");
    opts.addOption("help", false, "Print usage");
  }

  private void printUsage() {
    new HelpFormatter().printHelp("TonyClient", opts);
  }

  public boolean init(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args, true);
    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }

    initTonyConf(tonyConf, cliParser);

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

    yarnConfAddress = tonyConf.get(TonyConfigurationKeys.YARN_CONF_LOCATION);
    hdfsConfAddress = tonyConf.get(TonyConfigurationKeys.HDFS_CONF_LOCATION);
    taskParams = cliParser.getOptionValue("task_params");
    pythonBinaryPath = cliParser.getOptionValue("python_binary_path");
    pythonVenv = cliParser.getOptionValue("python_venv");
    executes = cliParser.getOptionValue("executes");

    // src_dir & hdfs_classpath flags are for compatibility.
    srcDir = cliParser.getOptionValue("src_dir");
    hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                                         + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                                         + " Specified virtual cores=" + amVCores);
    }

    int numWorkers = tonyConf.getInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME),
        TonyConfigurationKeys.getDefaultInstances(Constants.WORKER_JOB_NAME));
    boolean singleNode = tonyConf.getBoolean(TonyConfigurationKeys.IS_SINGLE_NODE,
        TonyConfigurationKeys.DEFAULT_IS_SINGLE_NODE);
    if (!singleNode) {
      if (numWorkers < 1) {
        throw new IllegalArgumentException(
            "Cannot request non-positive worker instances. Requested numWorkers=" + numWorkers);
      }
      if (amGpus > 0) {
        LOG.warn("It seems you reserved " + amGpus + " GPUs in application master (driver, which doesn't perform training) during distributed training.");
      }
    }

    appTimeout = tonyConf.getInt(TonyConfigurationKeys.APPLICATION_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_APPLICATION_TIMEOUT);

    if (cliParser.hasOption("shell_env")) {
      String[] envs = cliParser.getOptionValues("shell_env");
      shellEnv.putAll(Utils.parseKeyValue(envs));
    }

    if (tonyConf.getBoolean(TonyConfigurationKeys.DOCKER_ENABLED, TonyConfigurationKeys.DEFAULT_DOCKER_ENABLED)) {
      String imagePath = tonyConf.get(TonyConfigurationKeys.DOCKER_IMAGE);
      if (imagePath == null) {
        LOG.error("Docker is enabled but " + TonyConfigurationKeys.DOCKER_IMAGE + " is not set.");
        return false;
      } else {
        containerEnv.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "docker");
        containerEnv.put(DockerLinuxContainerRuntime.ENV_DOCKER_CONTAINER_IMAGE, imagePath);
      }
    }

    if (cliParser.hasOption("container_env")) {
      String[] containerEnvs = cliParser.getOptionValues("container_env");
      containerEnv.putAll(Utils.parseKeyValue(containerEnvs));
    }
    createYarnClient();
    return true;
  }

  /**
   * Add resource if exist to {@code tonyConf}
   * @param tonyConf Configuration object.
   * @param cliParser CommandLine object that has all the command line arguments.
   */
  public static void initTonyConf(Configuration tonyConf, CommandLine cliParser) {
    if (System.getenv(Constants.TONY_CONF_DIR) != null) {
      tonyConf.addResource(new Path(System.getenv(Constants.TONY_CONF_DIR) + File.separatorChar + Constants.TONY_SITE_CONF));
    }
    tonyConf.addResource(Constants.TONY_DEFAULT_XML);
    if (cliParser.hasOption("conf_file")) {
      tonyConf.addResource(new Path(cliParser.getOptionValue("conf_file")));
    } else {
      tonyConf.addResource(Constants.TONY_XML);
    }
    if (cliParser.hasOption("conf")) {
      String[] confs = cliParser.getOptionValues("conf");
      for (Map.Entry<String, String> cliConf : Utils.parseKeyValue(confs).entrySet()) {
        tonyConf.set(cliConf.getKey(), cliConf.getValue());
      }
    }
  }

  public Configuration getTonyConf() {
    return this.tonyConf;
  }

  public ContainerLaunchContext createAMContainerSpec(ApplicationId appId, long amMemory, String taskParams,
                                                      String pythonBinaryPath, String pythonVenv, String executes,
                                                      ByteBuffer tokens) throws IOException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    FileSystem fs = FileSystem.get(hdfsConf);
    appResourcesPath = new Path(fs.getHomeDirectory(), Constants.TONY_FOLDER + Path.SEPARATOR + appId.toString());
    Map<String, LocalResource> localResources = new HashMap<>();
    addLocalResources(fs, archivePath, LocalResourceType.FILE, Constants.TONY_ZIP_NAME, localResources);
    addLocalResources(fs, tonyFinalConfPath, LocalResourceType.FILE, Constants.TONY_FINAL_XML, localResources);
    String[] amResources = tonyConf.getStrings(TonyConfigurationKeys.getResourcesKey(Constants.AM_NAME));
    if (null != amResources) {
      for (String dir : amResources) {
        Utils.addResource(dir, localResources, hdfsConf);
      }
    }
    Utils.addResource(hdfsClasspath, localResources, hdfsConf);
    setAMEnvironment(localResources, fs);

    // Update absolute path with relative path
    if (hdfsConfAddress != null) {
      if (hdfsConfAddress.charAt(0) == '/') {
        String hc = hdfsConfAddress.substring(1);
        containerEnv.put(Constants.HDFS_CONF_PATH, hc);
      } else {
        containerEnv.put(Constants.HDFS_CONF_PATH, hdfsConfAddress);
      }
    }
    if (yarnConfAddress != null) {
      if (yarnConfAddress.charAt(0) == '/') {
        String yc = yarnConfAddress.substring(1);
        containerEnv.put(Constants.YARN_CONF_PATH, yc);
      } else {
        containerEnv.put(Constants.YARN_CONF_PATH, yarnConfAddress);
      }
    }

    // Set logs to be readable by everyone. Set app to be modifiable only by app owner.
    Map<ApplicationAccessType, String> acls = new HashMap<>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    acls.put(ApplicationAccessType.MODIFY_APP, " ");
    amContainer.setApplicationACLs(acls);

    String command = TonyClient.buildCommand(amMemory, taskParams, pythonBinaryPath, pythonVenv,
        executes, hdfsClasspath, shellEnv, containerEnv);

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
  static String buildCommand(long amMemory, String taskParams, String pythonBinaryPath, String pythonVenv,
      String executes, String hdfsClasspath, Map<String, String> shellEnv, Map<String, String> containerEnv) {
    List<String> arguments = new ArrayList<>(30);
    arguments.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    arguments.add("-Xmx" + (int) (amMemory * 0.8f) + "m");
    // Add configuration for log dir to retrieve log output from python subprocess in AM
    arguments.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Set class name
    arguments.add("com.linkedin.tony.TonyApplicationMaster");

    if (taskParams != null) {
      arguments.add("--task_params " + "'" + String.valueOf(taskParams) + "'");
    }
    if (pythonBinaryPath != null) {
      arguments.add("--python_binary_path " + String.valueOf(pythonBinaryPath));
    }
    if (pythonVenv != null) {
      arguments.add("--python_venv " + FilenameUtils.getName(String.valueOf(pythonVenv)));
    }
    if (executes != null) {
      arguments.add("--executes " + String.valueOf(executes));
    }
    if (hdfsClasspath != null) {
      arguments.add("--hdfs_classpath " + String.valueOf(hdfsClasspath));
    }
    for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
      arguments.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : containerEnv.entrySet()) {
      arguments.add("--container_env " + entry.getKey() + "=" + entry.getValue());
    }
    arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDOUT_FILENAME);
    arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDERR_FILENAME);
    return String.join(" ", arguments);
  }

  /**
   * Create zip archive required by TonY to distribute python code and virtual environment
   */
  private void zipArchive() throws IOException {
    FileOutputStream fos = new FileOutputStream(archivePath);
    ZipOutputStream zos = new ZipOutputStream(fos);
    // Accept archive file as srcDir.
    if (!Utils.isArchive(srcDir)) {
      addDirToZip(zos, srcDir);
    } else {
      Utils.renameFile(srcDir, archivePath);
    }
    if (hdfsConfAddress != null) {
      addFileToZip(zos, hdfsConfAddress);
    }
    if (yarnConfAddress != null) {
      addFileToZip(zos, yarnConfAddress);
    }
    if (pythonVenv != null) {
      addFileToZip(zos, pythonVenv);
    }
    zos.close();
  }

  private static void addDirToZip(ZipOutputStream zos, String dirName) throws IOException {
    File f = new File(dirName);
    if (!f.isDirectory()) {
      throw new IOException(dirName + " is not a valid directory.");
    }
    for (File content : f.listFiles()) {
      if (content.isDirectory()) {
        addDirToZip(zos, content.getPath());
      } else {
        addFileToZip(zos, content.getPath());
      }
    }
  }

  private static void addFileToZip(ZipOutputStream zos, String filePath) throws IOException {
    zos.putNextEntry(new ZipEntry(filePath));
    byte[] buf = new byte[2048];

    try (FileInputStream fos = new FileInputStream(new File(filePath))) {
      int readBytes;
      while ((readBytes = fos.read(buf)) > 0) {
        zos.write(buf, 0, readBytes);
      }
    }
    zos.closeEntry();
  }

  /**
   * Add a local resource to HDFS and local resources map.
   * @param fs HDFS file system reference
   * @param resourceType the type of the src file
   * @param dstPath name of the resource after localization
   * @param localResources the local resources map
   * @throws IOException error when writing to HDFS
   */
  private void addLocalResources(FileSystem fs, String srcPath, LocalResourceType resourceType,
                                 String dstPath, Map<String, LocalResource> localResources) throws IOException {
    Path dst = new Path(appResourcesPath, dstPath);
    fs.copyFromLocalFile(new Path(srcPath), dst);
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
    LocalResource zipResource = localResources.get(Constants.TONY_ZIP_NAME);
    Utils.addEnvironmentForResource(zipResource, fs, Constants.TONY_ZIP_PREFIX, containerEnv);

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
      String tokenRenewer = YarnClientUtils.getRmPrincipal(yarnConf);
      if (tokenRenewer == null) {
        throw new RuntimeException("Failed to get RM principal.");
      }
      final Token<?> rmToken = ConverterUtils.convertFromYarn(yarnClient.getRMDelegationToken(new Text(tokenRenewer)),
                                                     yarnConf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                                                                            YarnConfiguration.DEFAULT_RM_ADDRESS,
                                                                            YarnConfiguration.DEFAULT_RM_PORT));
      LOG.info("RM delegation token fetched.");
      String defaultFS = hdfsConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
          CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
      LOG.info("Fetching HDFS delegation token for default namenode: " + defaultFS);
      FileSystem fs = FileSystem.get(hdfsConf);
      final Token<?> fsToken = fs.getDelegationToken(tokenRenewer);
      if (fsToken == null) {
        throw new RuntimeException("Failed to get FS delegation token for default FS.");
      }
      LOG.info("Default HDFS delegation token fetched.");
      cred.addToken(rmToken.getService(), rmToken);
      cred.addToken(fsToken.getService(), fsToken);
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
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   */
  private boolean monitorApplication(ApplicationId appId)
      throws YarnException, IOException, InterruptedException {

    while (true) {
      // Check app status every 1 second.
      Thread.sleep(1000);

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      initRpcClient(report);

      // Query AM for taskUrls if taskUrls is empty.
      if (amRpcServerInitialized && taskUrls.isEmpty()) {
        taskUrls = amRpcClient.getTaskUrls();
        if (!taskUrls.isEmpty()) {
          // Print TaskUrls
          new TreeSet<>(taskUrls).forEach(task -> Utils.printTaskUrl(task, LOG));
        }
      }

      if (YarnApplicationState.FINISHED == state || YarnApplicationState.FAILED == state
          || YarnApplicationState.KILLED == state) {
        LOG.info("Application " + appId.getId() + " finished with YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ", breaking monitoring loop.");
        // Set amRpcClient to null so client does not try to connect to it after completion.
        amRpcClient = null;
        return FinalApplicationStatus.SUCCEEDED == dsStatus;
      }

      if (appTimeout > 0) {
        if (System.currentTimeMillis() > (clientStartTime + appTimeout)) {
          LOG.info("Reached client specified timeout for application. Killing application"
                   + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
          forceKillApplication(appId);
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
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   */
  private void forceKillApplication(ApplicationId appId)
      throws YarnException, IOException {
    yarnClient.killApplication(appId);

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
    } catch (IOException | InterruptedException | URISyntaxException | YarnException e) {
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

  public static void main(String[] args) {
    int exitCode = 0;
    try (TonyClient client = new TonyClient(new Configuration())) {
      boolean sanityCheck = client.init(args);
      if (!sanityCheck) {
        LOG.fatal("Failed to parse arguments.");
      }
      exitCode = client.start();
      if (client.amRpcClient != null) {
        client.amRpcClient.finishApplication();
      }
    } catch (ParseException | IOException | YarnException e) {
      LOG.fatal("Failed to init client.", e);
      System.exit(-1);
    }
    System.exit(exitCode);
  }

}
