/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.linkedin.tony.Constants;
import com.linkedin.tony.LocalizableResource;
import com.linkedin.tony.TFConfig;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.tensorflow.TensorFlowContainerRequest;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Utils {
  private static final Log LOG = LogFactory.getLog(Utils.class);

  private static final String WORKER_LOG_URL_TEMPLATE = "http://%s/node/containerlogs/%s/%s";

  /**
   * Poll a callable till it returns true or time out
   * @param func a function that returns a boolean
   * @param interval the interval we poll (in seconds).
   * @param timeout the timeout we will stop polling (in seconds).
   * @return if the func returned true before timing out.
   * @throws IllegalArgumentException if {@code interval} or {@code timeout} is negative
   */
  public static boolean poll(Callable<Boolean> func, int interval, int timeout) {
    Preconditions.checkArgument(interval >= 0, "Interval must be non-negative.");
    Preconditions.checkArgument(timeout >= 0, "Timeout must be non-negative.");

    int remainingTime = timeout;
    try {
      while (timeout == 0 || remainingTime >= 0) {
        if (func.call()) {
          LOG.info("Poll function finished within " + timeout + " seconds");
          return true;
        }
        Thread.sleep(interval * 1000);
        remainingTime -= interval;
      }
    } catch (Exception e) {
      LOG.error("Polled function threw exception.", e);
    }
    LOG.warn("Function didn't return true within " + timeout + " seconds.");
    return false;
  }

  /**
   * Polls the function {@code func} every {@code interval} seconds until the function returns non-null or until
   * {@code timeout} seconds is reached, and which point this function returns null. If {@code timeout} is 0, the
   * function will be polled forever until it returns non-null.
   *
   * @param func  the function to poll
   * @param interval  the interval, in seconds, at which to poll the function
   * @param timeout  the maximum time to poll for until giving up and returning null
   * @param <T>  the type of the object returned by the function
   * @return  the non-null object returned by the function or null if {@code timeout} is reached
   * @throws IllegalArgumentException  if {@code interval} or {@code timeout} is negative
   */
  public static <T> T pollTillNonNull(Callable<T> func, int interval, int timeout) {
    Preconditions.checkArgument(interval >= 0, "Interval must be non-negative.");
    Preconditions.checkArgument(timeout >= 0, "Timeout must be non-negative.");

    int remainingTime = timeout;
    T ret;
    try {
      while (timeout == 0 || remainingTime >= 0) {
        ret = func.call();
        if (ret != null) {
          LOG.info("pollTillNonNull function finished within " + timeout + " seconds");
          return ret;
        }
        Thread.sleep(interval * 1000);
        remainingTime -= interval;
      }
    } catch (Exception e) {
      LOG.error("pollTillNonNull function threw exception", e);
    }
    LOG.warn("Function didn't return non-null within " + timeout + " seconds.");
    return null;
  }

  public static String parseMemoryString(String memory) {
    memory = memory.toLowerCase();
    int m = memory.indexOf('m');
    int g = memory.indexOf('g');
    if (-1 != m) {
      return memory.substring(0, m);
    }
    if (-1 != g) {
      return String.valueOf(Integer.parseInt(memory.substring(0, g)) * 1024);
    }
    return memory;
  }

  public static void zipFolder(java.nio.file.Path sourceFolderPath, java.nio.file.Path zipPath) throws IOException {
    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()));
    Files.walkFileTree(sourceFolderPath, new SimpleFileVisitor<java.nio.file.Path>() {
      public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
        zos.putNextEntry(new ZipEntry(sourceFolderPath.relativize(file).toString()));
        Files.copy(file, zos);
        zos.closeEntry();
        return FileVisitResult.CONTINUE;
      }
    });
    zos.close();
  }

  public static void unzipArchive(String src, String dst) {
    LOG.info("Unzipping " + src + " to destination " + dst);
    try {
      ZipFile zipFile = new ZipFile(src);
      zipFile.extractAll(dst);
    } catch (ZipException e) {
      LOG.fatal("Failed to unzip " + src, e);
    }
  }

  /**
   * Uses reflection to set GPU capability if GPU support is available.
   * @param resource  the resource to set GPU capability on
   * @param gpuCount  the number of GPUs requested
   */
  public static void setCapabilityGPU(Resource resource, int gpuCount) {
    // short-circuit when the GPU count is 0.
    if (gpuCount <= 0) {
      return;
    }
    try {
      Method method = resource.getClass().getMethod(Constants.SET_RESOURCE_VALUE_METHOD, String.class, long.class);
      method.invoke(resource, Constants.GPU_URI, gpuCount);
    } catch (NoSuchMethodException nsme) {
      LOG.error("There is no '" + Constants.SET_RESOURCE_VALUE_METHOD + "' API in this version ("
              + VersionInfo.getVersion() + ") of YARN", nsme);
      throw new RuntimeException(nsme);
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOG.error("Failed to invoke '" + Constants.SET_RESOURCE_VALUE_METHOD + "' method to set GPU resources", e);
      throw new RuntimeException(e);
    }
    return;
  }

  public static String constructUrl(String urlString) {
    if (!urlString.startsWith("http")) {
      return "http://" + urlString;
    }
    return urlString;
  }

  public static String constructContainerUrl(Container container) {
    return constructContainerUrl(container.getNodeHttpAddress(), container.getId());
  }

  public static String constructContainerUrl(String nodeAddress, ContainerId containerId) {
    try {
      return String.format(WORKER_LOG_URL_TEMPLATE, nodeAddress, containerId,
              UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void printTaskUrl(TaskInfo taskInfo, Log log) {
    log.info(String.format("Logs for %s %s at: %s", taskInfo.getName(), taskInfo.getIndex(), taskInfo.getUrl()));
  }

  public static void printTonyPortalUrl(String portalUrl, String appId, Log log) {
    log.info(String.format("Link for %s's events/metrics: %s/%s/%s", appId, portalUrl, Constants.JOBS_SUFFIX, appId));
  }

  /**
   * Parse a list of env key-value pairs like PATH=ABC to a map of key value entries.
   * @param keyValues the input key value pairs
   * @return a map contains the key value {"PATH": "ABC"}
   */
  public static Map<String, String> parseKeyValue(String[] keyValues) {
    Map<String, String> keyValue = new HashMap<>();
    if (keyValues == null) {
      return keyValue;
    }
    for (String kv : keyValues) {
      String trimmedKeyValue = kv.trim();
      int index = kv.indexOf('=');
      if (index == -1) {
        keyValue.put(trimmedKeyValue, "");
        continue;
      }
      String key = trimmedKeyValue.substring(0, index);
      String val = "";
      if (index < (trimmedKeyValue.length() - 1)) {
        val = trimmedKeyValue.substring(index + 1);
      }
      keyValue.put(key, val);
    }
    return keyValue;
  }

  /**
   * This function is used by ApplicationMaster and TonyClient to set up
   * common command line arguments.
   * @return Options that contains common options
   */
  public static Options getCommonOptions() {
    Options opts = new Options();

    // Container environment
    opts.addOption("hdfs_classpath", true, "Path to jars on HDFS for workers.");

    // Python env
    opts.addOption("python_binary_path", true, "The relative path to python binary.");
    opts.addOption("python_venv", true, "The python virtual environment zip.");

    return opts;
  }

  /**
   * Execute a shell command.
   * @param taskCommand the shell command to execute
   * @param timeout the timeout to stop running the shell command
   * @param env the environment for this shell command
   * @return the exit code of the shell command
   * @throws IOException
   * @throws InterruptedException
   */
  public static int executeShell(String taskCommand, long timeout, Map<String, String> env) throws IOException, InterruptedException {
    LOG.info("Executing command: " + taskCommand);
    String executablePath = taskCommand.trim().split(" ")[0];
    File executable = new File(executablePath);
    if (!executable.canExecute()) {
      if (!executable.setExecutable(true)) {
        LOG.error("Failed to make " + executable + " executable");
      }
    }

    // Used for running unit tests in build boxes without Hadoop environment.
    if (System.getenv(Constants.SKIP_HADOOP_PATH) == null) {
      taskCommand = Constants.HADOOP_CLASSPATH_COMMAND + taskCommand;
    }
    ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", taskCommand);
    taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    if (env != null) {
      taskProcessBuilder.environment().putAll(env);
    }
    Process taskProcess = taskProcessBuilder.start();
    if (timeout > 0) {
      taskProcess.waitFor(timeout, TimeUnit.MILLISECONDS);
    } else {
      taskProcess.waitFor();
    }
    return taskProcess.exitValue();

  }

  public static String getCurrentHostName() {
    return System.getenv(ApplicationConstants.Environment.NM_HOST.name());
  }

  public static String getHostNameOrIpFromTokenConf(Configuration conf)
      throws YarnException, SocketException {
    boolean useIp = conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
    String hostName =
        System.getenv(ApplicationConstants.Environment.NM_HOST.name());
    if (useIp) {
      InetAddress ip = NetUtils.getLocalInetAddress(hostName);
      if (ip == null) {
        throw new YarnException("Can't resolve the ip of " + hostName);
      }
      return ip.getHostAddress();
    } else {
      return hostName;
    }
  }

  public static void addEnvironmentForResource(LocalResource resource, FileSystem fs, String envPrefix,
                                               Map<String, String> env) throws IOException {
    Path resourcePath = new Path(fs.getHomeDirectory(), resource.getResource().getFile());
    FileStatus resourceStatus = fs.getFileStatus(resourcePath);
    long resourceLength = resourceStatus.getLen();
    long resourceTimestamp = resourceStatus.getModificationTime();

    env.put(envPrefix + Constants.PATH_SUFFIX, resourcePath.toString());
    env.put(envPrefix + Constants.LENGTH_SUFFIX, Long.toString(resourceLength));
    env.put(envPrefix + Constants.TIMESTAMP_SUFFIX, Long.toString(resourceTimestamp));
  }

  /**
   * Parses resource requests from configuration of the form "tony.x.y" where "x" is the
   * TensorFlow job name, and "y" is "instances" or the name of a resource type
   * (i.e. memory, vcores, gpus).
   * @param conf the TonY configuration.
   * @return map from configured job name to its corresponding resource request
   */
  public static Map<String, TensorFlowContainerRequest> parseContainerRequests(Configuration conf) {
    Set<String> jobNames = getAllJobTypes(conf);
    Map<String, TensorFlowContainerRequest> containerRequests = new HashMap<>();
    int priority = 0;
    for (String jobName : jobNames) {
      int numInstances = conf.getInt(TonyConfigurationKeys.getInstancesKey(jobName), 0);
      String memoryString = conf.get(TonyConfigurationKeys.getResourceKey(jobName, Constants.MEMORY),
              TonyConfigurationKeys.DEFAULT_MEMORY);
      long memory = Long.parseLong(parseMemoryString(memoryString));
      int vCores = conf.getInt(TonyConfigurationKeys.getResourceKey(jobName, Constants.VCORES),
              TonyConfigurationKeys.DEFAULT_VCORES);
      int gpus = conf.getInt(TonyConfigurationKeys.getResourceKey(jobName, Constants.GPUS),
              TonyConfigurationKeys.DEFAULT_GPUS);

      /* The priority of different task types MUST be different.
       * Otherwise the requests will overwrite each other on the RM
       * scheduling side. See YARN-7631 for details.
       * For now we set the priorities of different task types arbitrarily.
       */
      if (numInstances > 0) {
        // We rely on unique priority behavior to match allocation request to task in Hadoop 2.7
        containerRequests.put(jobName,
                new TensorFlowContainerRequest(jobName, numInstances, memory, vCores, gpus, priority));
        priority++;
      }
    }
    return containerRequests;
  }

  public static Set<String> getAllJobTypes(Configuration conf) {
    return conf.getValByRegex(TonyConfigurationKeys.INSTANCES_REGEX).keySet().stream()
        .map(Utils::getTaskType)
        .collect(Collectors.toSet());
  }

  /**
   * Extracts TensorFlow job name from configuration key of the form "tony.*.instances".
   * @param confKey Name of the configuration key
   * @return TensorFlow job name
   */
  public static String getTaskType(String confKey) {
    Pattern instancePattern = Pattern.compile(TonyConfigurationKeys.INSTANCES_REGEX);
    Matcher instanceMatcher = instancePattern.matcher(confKey);
    if (instanceMatcher.matches()) {
      return instanceMatcher.group(1);
    } else {
      return null;
    }
  }

  public static boolean isArchive(String path) {
    File f = new File(path);
    int fileSignature = 0;
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(f, "r");
      fileSignature = raf.readInt();
    } catch (IOException e) {
      // handle if you like
    } finally {
      IOUtils.closeQuietly(raf);
    }
    return fileSignature == 0x504B0304 // zip
            || fileSignature == 0x504B0506 // zip
            || fileSignature == 0x504B0708 // zip
            || fileSignature == 0x74657374 // tar
            || fileSignature == 0x75737461 // tar
            || (fileSignature & 0xFFFF0000) == 0x1F8B0000; // tar.gz
  }

  public static boolean renameFile(String oldName, String newName) {
    File oldFile = new File(oldName);
    File newFile = new File(newName);
    return oldFile.renameTo(newFile);
  }

  public static String constructTFConfig(String clusterSpec, String jobName, int taskIndex) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, List<String>> spec =
              mapper.readValue(clusterSpec, new TypeReference<Map<String, List<String>>>() { });
      TFConfig tfConfig = new TFConfig(spec, jobName, taskIndex);
      return mapper.writeValueAsString(tfConfig);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static String getClientResourcesPath(String appId, String fileName) {
    return String.format("%s-%s", appId, fileName);
  }

  public static void cleanupHDFSPath(Configuration hdfsConf, Path path) {
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      if (path != null && fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (IOException e) {
      LOG.error("Failed to clean up HDFS path: " + path, e);
    }
  }

  /**
   * Adds the resources in {@code resources} to the {@code resourcesMap}.
   * @param resources  List of resource paths to process. If a resource is a directory,
   *                   its immediate files will be added.
   * @param resourcesMap  map where resource path to {@Link LocalResource} mapping will be added
   * @param fs  {@link FileSystem} used to list the resources
   */
  public static void addResources(String[] resources, Map<String, LocalResource> resourcesMap, FileSystem fs) {
    if (null != resources) {
      for (String dir : resources) {
        Utils.addResource(dir, resourcesMap, fs);
      }
    }
  }

  /**
   * Add files inside a path to local resources. If the path is a directory, its first level files will be added
   * to the local resources. Note that we don't add nested files.
   * @param path the directory whose contents will be localized.
   * @param resourcesMap map where resource path to {@link LocalResource} mapping will be added
   * @param fs the filesystem instance used to read the {@code path}.
   */
  public static void addResource(String path, Map<String, LocalResource> resourcesMap, FileSystem fs) {
    try {
      if (path != null) {
        // Check the format of the path, if the path is of path#archive, we set resource type as ARCHIVE
        LocalizableResource localizableResource = new LocalizableResource(path, fs);
        if (localizableResource.isDirectory()) {
          Path dirpath = localizableResource.getSourceFilePath();
          FileStatus[] ls = fs.listStatus(dirpath);
          for (FileStatus fileStatus : ls) {
            // We only add first level files.
            if (fileStatus.isDirectory()) {
              continue;
            }
            addResource(fileStatus.getPath().toString(), resourcesMap, fs);
          }
        } else {
          resourcesMap.put(localizableResource.getLocalizedFileName(), localizableResource.toLocalResource());
        }
      }
    } catch (IOException | ParseException exception) {
      LOG.error("Failed to add " + path + " to local resources.", exception);
    }
  }

  public static String buildRMUrl(Configuration yarnConf, String appId) {
    return "http://" + yarnConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS) + "/cluster/app/" + appId;
  }

  public static void printWorkerTasksCompleted(AtomicInteger completedWTasks, long totalWTasks) {
    if (completedWTasks.get() == totalWTasks) {
      LOG.info("Completed all " + totalWTasks + " worker tasks.");
      return;
    }
    LOG.info("Completed worker tasks: " + completedWTasks.get() + " out of " + totalWTasks + " worker tasks.");
  }

  public static String parseClusterSpecForPytorch(String clusterSpec) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, List<String>> clusterSpecMap =
            objectMapper.readValue(clusterSpec, new TypeReference<Map<String, List<String>>>() { });
    String chiefWorkerAddress = clusterSpecMap.get(Constants.WORKER_JOB_NAME).get(0);
    if (chiefWorkerAddress == null) {
      LOG.error("Failed to get chief worker address from cluster spec.");
      return null;
    }
    return Constants.COMMUNICATION_BACKEND + chiefWorkerAddress;
  }

  public static void createDirIfNotExists(FileSystem fs, Path dir, FsPermission permission) {
    String warningMsg;
    try {
      if (!fs.exists(dir)) {
        fs.mkdirs(dir, permission);
        fs.setPermission(dir, permission);
        return;
      }
      warningMsg = "Directory " + dir + " already exists!";
      LOG.info(warningMsg);
    } catch (IOException e) {
      warningMsg = "Failed to create " + dir + ": " + e.toString();
      LOG.error(warningMsg);
    }
  }

  public static String[] getUntrackedJobTypes(Configuration conf) {
    return conf.getStrings(TonyConfigurationKeys.UNTRACKED_JOBTYPES, TonyConfigurationKeys.UNTRACKED_JOBTYPES_DEFAULT);
  }

  public static boolean isJobTypeTracked(String taskName, Configuration tonyConf) {
    return !Arrays.asList(getUntrackedJobTypes(tonyConf)).contains(taskName);
  }

  public static void uploadFileAndSetConfResources(Path hdfsPath, Path filePath, String fileName,
                                                   Configuration tonyConf, FileSystem fs,
                                                   LocalResourceType resourceType, String resourceKey) throws IOException {
    Path dst = new Path(hdfsPath, fileName);
    HdfsUtils.copySrcToDest(filePath, dst, tonyConf);
    fs.setPermission(dst, new FsPermission((short) 0770));
    String dstAddress = dst.toString();
    if (resourceType == LocalResourceType.ARCHIVE) {
      dstAddress += Constants.ARCHIVE_SUFFIX;
    }
    appendConfResources(resourceKey, dstAddress, tonyConf);
  }

  public static void appendConfResources(String key, String resource, Configuration tonyConf) {
    if (resource == null) {
      return;
    }
    String[] resources = tonyConf.getStrings(key);
    List<String> updatedResources = new ArrayList<>();
    if (resources != null) {
      updatedResources = new ArrayList<>(Arrays.asList(resources));
    }
    updatedResources.add(resource);
    tonyConf.setStrings(key, updatedResources.toArray(new String[0]));
  }

  public static void initYarnConf(Configuration yarnConf) {
    addCoreConfs(yarnConf);
    addComponentConfs(yarnConf, Constants.YARN_DEFAULT_CONF, Constants.YARN_SITE_CONF);
  }

  public static void initHdfsConf(Configuration hdfsConf) {
    addCoreConfs(hdfsConf);
    addComponentConfs(hdfsConf, Constants.HDFS_DEFAULT_CONF, Constants.HDFS_SITE_CONF);
  }

  private static void addCoreConfs(Configuration conf) {
    URL coreDefault = Utils.class.getClassLoader().getResource(Constants.CORE_DEFAULT_CONF);
    if (coreDefault != null) {
      conf.addResource(coreDefault);
    }
    if (new File(Constants.CORE_SITE_CONF).exists()) {
      conf.addResource(new Path(Constants.CORE_SITE_CONF));
    }
  }

  private static void addComponentConfs(Configuration conf, String defaultConfName, String siteConfName) {
    URL defaultConf = Utils.class.getClassLoader().getResource(defaultConfName);
    if (defaultConf != null) {
      conf.addResource(defaultConf);
    }
    if (new File(siteConfName).exists()) {
      conf.addResource(new Path(siteConfName));
    }
  }

  public static void extractResources() {
    if (new File(Constants.TONY_SRC_ZIP_NAME).exists()) {
      LOG.info("Unpacking src directory..");
      Utils.unzipArchive(Constants.TONY_SRC_ZIP_NAME, "./");
    }
    File venvZip = new File(Constants.PYTHON_VENV_ZIP);
    if (venvZip.exists() && venvZip.isFile()) {
      LOG.info("Unpacking Python virtual environment.. ");
      Utils.unzipArchive(Constants.PYTHON_VENV_ZIP, Constants.PYTHON_VENV_DIR);
    } else {
      LOG.info("No virtual environment uploaded.");
    }
  }

  private Utils() { }
}
