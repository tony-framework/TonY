/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.tony.Constants;
import com.linkedin.tony.Utils;
import com.linkedin.tony.rpc.TaskUrl;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static com.linkedin.tony.Constants.*;


/**
 * Represents a Tensorflow session.
 */
public class TensorFlowSession {
  private static final Log LOG = LogFactory.getLog(TensorFlowSession.class);

  private String taskCmd;

  private String venv;
  private String amAddress;
  private int numWorkers;
  private int numPs;

  // sessionId to distinguish different sessions. Currently used to distinguish
  // failed session and new session.
  public int sessionId = 0;

  // A map from task name to an array of TFTasks with that name.
  private Map<String, TFTask[]> jobTasks = new ConcurrentHashMap<>();

  private FinalApplicationStatus sessionFinalStatus = FinalApplicationStatus.UNDEFINED;
  private String sessionFinalMessage = null;
  private TensorFlowContainerRequest psContainerRequest;
  private Map<String, String> shellEnv;
  private String jvmArgs;
  private TensorFlowContainerRequest workerContainerRequest;
  private Map<String, Set<Long>> jobTypeToAllocationIds = new HashMap<String, Set<Long>>();

  public enum TaskType {
    TASK_TYPE_CHIEF, TASK_TYPE_PARAMETER_SERVER, TASK_TYPE_OTHERS
  }

  public String getTaskCommand() {
    StringBuilder cmd = new StringBuilder();
    cmd.append("$JAVA_HOME/bin/java ")
        .append(jvmArgs)
        .append(" com.linkedin.tony.TaskExecutor ")
        .append(" --am_address ")
        .append(amAddress)
        .append(" --task_command ")
        .append(taskCmd);
    if (venv != null) {
      cmd.append(" --venv ");
      cmd.append(venv);
    }
    for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
      cmd.append(" --shell_env ");
      cmd.append(entry.getKey());
      cmd.append("=");
      cmd.append(entry.getValue());
    }
    return cmd.toString();
  }

  private Map<ContainerId, TFTask> containerIdMap = new HashMap<>();

  public TensorFlowSession() {
  }

  private TensorFlowSession(Builder builder) {
    this.taskCmd = builder.taskCmd;
    this.venv = builder.venv;
    this.amAddress = builder.amAddress;
    this.psContainerRequest = builder.psContainerRequest;
    this.workerContainerRequest = builder.workerContainerRequest;
    this.shellEnv = builder.shellEnv;
    this.jvmArgs = builder.jvmArgs;

    this.numWorkers = builder.numWorkers;
    this.numPs = builder.numPs;
    TFTask[] workerTasks = new TFTask[this.numWorkers];
    jobTasks.put(WORKER_JOB_NAME, workerTasks);


    TFTask[] psTasks = new TFTask[this.numPs];
    jobTasks.put(PS_JOB_NAME, psTasks);
  }

  public Map<String, TFTask[]> getTFTasks() {
    return this.jobTasks;
  }

  public void setResources(Configuration yarnConf,
                           Configuration hdfsConf,
                           Map<String, LocalResource> localResources,
                           Map<String, String> shellEnv,
                           String hdfsClasspathDir) {

    Map<String, String> env = System.getenv();
    String zipPath = env.get(Constants.TF_ZIP_PREFIX + Constants.PATH_SUFFIX);
    long zipTimestamp = Long.valueOf(env.get(Constants.TF_ZIP_PREFIX + Constants.TIMESTAMP_SUFFIX));
    long zipLength = Long.valueOf(env.get(Constants.TF_ZIP_PREFIX + Constants.LENGTH_SUFFIX));

    LocalResource zipResource =
        LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(URI.create(zipPath)),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE,
            zipLength, zipTimestamp);
    localResources.put(Constants.TF_ZIP_NAME, zipResource);

    String tonyConfPath = env.get(Constants.TONY_CONF_PREFIX + Constants.PATH_SUFFIX);
    long tonyConfTimestamp = Long.valueOf(env.get(Constants.TONY_CONF_PREFIX + Constants.TIMESTAMP_SUFFIX));
    long tonyConfLength = Long.valueOf(env.get(Constants.TONY_CONF_PREFIX + Constants.LENGTH_SUFFIX));

    LocalResource tonyConfResource =
        LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(URI.create(tonyConfPath)),
            LocalResourceType.FILE, LocalResourceVisibility.PRIVATE,
            tonyConfLength, tonyConfTimestamp);
    localResources.put(Constants.TONY_FINAL_XML, tonyConfResource);

    try {
      if (hdfsClasspathDir != null) {
        FileSystem fs = FileSystem.get(new URI(hdfsClasspathDir), hdfsConf);
        FileStatus[] ls = fs.listStatus(new Path(hdfsClasspathDir));
        for (FileStatus jar : ls) {
          LocalResource resource =
              LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(URI.create(jar.getPath().toString())),
                                        LocalResourceType.FILE, LocalResourceVisibility.PRIVATE,
                                        jar.getLen(), jar.getModificationTime());

          localResources.put(jar.getPath().getName(), resource);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : yarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    shellEnv.put("CLASSPATH", classPathEnv.toString());
  }

  public synchronized List<TensorFlowContainerRequest> getContainersRequests() {
    List<TensorFlowContainerRequest> requests = new ArrayList<>();
    for (Map.Entry<String, TFTask[]> entry : jobTasks.entrySet()) {
      TFTask[] tasks = entry.getValue();
      for (TFTask task : tasks) {
        if (task == null) {
          requests.add(createContainerRequestForType(entry.getKey()));
        }
      }
    }
    return requests;
  }

  public TensorFlowContainerRequest createContainerRequestForType(String jobType) {
    switch (jobType) {
      case PS_JOB_NAME:
        return new TensorFlowContainerRequest(psContainerRequest);
      case WORKER_JOB_NAME:
        return new TensorFlowContainerRequest(workerContainerRequest);
      default:
        throw new IllegalArgumentException("Invalid job type: " + jobType);
    }
  }

  public boolean allTasksScheduled() {
    for (TFTask[] tasks : jobTasks.values()) {
      for (TFTask task : tasks) {
        if (task == null) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Associate an allocationRequestId to a TensorFlow job.
   * @param jobName the TensorFlow job name
   * @param allocationRequestId the allocationRequestId which corresponds to an instance of this job
   */
  public void addAllocationIdToJob(String jobName, long allocationRequestId) {
    if (jobTypeToAllocationIds.get(jobName) == null) {
      jobTypeToAllocationIds.put(jobName, new HashSet<>());
    }
    jobTypeToAllocationIds.get(jobName).add(allocationRequestId);
    LOG.info(String.format("Job %s with allocationRequestId %d", jobName, allocationRequestId));
  }

  /**
   * Get a TensorFlow task that hasn't been scheduled.
   * @param allocationRequestId the allocationRequestId of the allocated container
   * @return task to be assigned to this allocation
   */
  public synchronized TFTask getRemainingTask(long allocationRequestId) {
    for (Map.Entry<String, TFTask[]> entry : jobTasks.entrySet()) {
      String jobName = entry.getKey();
      if (!jobTypeToAllocationIds.get(jobName).contains(allocationRequestId)) {
        continue;
      }
      TFTask[] tasks = entry.getValue();
      for (int i = 0; i < tasks.length; i++) {
        if (tasks[i] == null) {
          tasks[i] = new TFTask(jobName, String.valueOf(i));
          LOG.info(String.format("Matched job %s with allocationRequestId %d", jobName, allocationRequestId));
          return tasks[i];
        }
      }
    }
    return null;
  }

  public Map<String, List<String>> getClusterSpec() {
    Map<String, List<String>> map = new HashMap<>();

    for (Map.Entry<String, TFTask[]> entry : jobTasks.entrySet()) {
      String jobName = entry.getKey();
      TFTask[] tasks = entry.getValue();

      List<String> builder = new ArrayList<>();
      for (TFTask task : tasks) {
        if (task == null) {
          continue;
        }

        String hostPort = task.getHostPort();
        builder.add(hostPort);
      }
      map.put(jobName, builder);
    }

    return map;
  }

  /**
   * Refresh task status on each TaskExecutor registers its exit code with AM.
   */
  public void onTaskCompleted(String jobName, String jobIndex, int exitCode) {
    TFTask task = getTask(jobName, jobIndex);
    Preconditions.checkNotNull(task);
    TaskType taskType = getTaskType(task);
    task.setExitStatus(exitCode);
    switch (taskType) {
      case TASK_TYPE_CHIEF:
      case TASK_TYPE_PARAMETER_SERVER:
      case TASK_TYPE_OTHERS:
        // On worker failure, set job to fail.
        if (exitCode != 0) {
          setFinalStatus(FinalApplicationStatus.FAILED, "Exit status: " + exitCode);
        }
        break;
      default:
        break;
    }
  }

  /**
   * Update the status of a session and set exit code if a session is completed.
   */
  public void updateSessionStatus() {
    int failureCount = 0;
    for (Map.Entry<String, TFTask[]> entry : jobTasks.entrySet()) {
      String jobName = entry.getKey();
      TFTask[] tasks = entry.getValue();

      if (jobName.equals(PS_JOB_NAME)) {
        // ignore PS job
        continue;
      }

      for (TFTask task : tasks) {
        if (task == null) {
          String msg = "Job is null, this should not happen.";
          LOG.error(msg);
          setFinalStatus(FinalApplicationStatus.FAILED, msg);
          return;
        }
        boolean isCompleted = task.isCompleted();
        if (!isCompleted) {
          String msg = "Job " + task.jobName + " at index: " + task.jobIndex + " haven't finished yet.";
          LOG.error(msg);
          setFinalStatus(FinalApplicationStatus.FAILED, msg);
          return;
        }

        int exitStatus = task.getExitStatus();
        if (exitStatus != 0) {
          failureCount++;
        }
      }
    }

    if (failureCount > 0) {
      setFinalStatus(FinalApplicationStatus.FAILED,
                     "At least one job task exited with non-zero status, failedCnt="
                     + failureCount);
    } else {
      LOG.info("Session completed with no job failures, setting final status SUCCEEDED.");
      setFinalStatus(FinalApplicationStatus.SUCCEEDED, null);
    }
  }

  public String getFinalMessage() {
    return sessionFinalMessage;
  }

  public FinalApplicationStatus getFinalStatus() {
    return sessionFinalStatus;
  }

  public void setFinalStatus(FinalApplicationStatus status, String message) {
    sessionFinalStatus = status;
    sessionFinalMessage = message;
  }

  private TaskType getTaskType(TFTask task) {
    TaskType type;
    String jobName = task.getJobName();
    if (jobName.equals(PS_JOB_NAME)) {
      type = TaskType.TASK_TYPE_PARAMETER_SERVER;
    } else {
      type = TaskType.TASK_TYPE_OTHERS;
    }
    return type;
  }

  private TFTask getTask(String jobName, String jobIndex) {
    for (Map.Entry<String, TFTask[]> entry : jobTasks.entrySet()) {
      TFTask[] tasks = entry.getValue();
      for (TFTask task : tasks) {
        String job = task.getJobName();
        String index = task.getJobIndex();
        if (job.equals(jobName) && index.equals(jobIndex)) {
          return task;
        }
      }
    }
    return null;
  }

  public TFTask getTask(ContainerId containerId) {
    return containerIdMap.get(containerId);
  }

  /**
   * Builder to compose the TensorFlowSession class.
   */
  public static class Builder {
    private String taskCmd;
    private int numWorkers;
    private int numPs;
    private String venv;
    private Map<String, String> shellEnv;
    private String amAddress;
    private TensorFlowContainerRequest psContainerRequest;
    private TensorFlowContainerRequest workerContainerRequest;
    private String jvmArgs;

    public TensorFlowSession build() {
      return new TensorFlowSession(this);
    }

    public Builder setNumWorkers(int numWorkers) {
      this.numWorkers = numWorkers;
      return this;
    }

    public Builder setNumPs(int numPs) {
      this.numPs = numPs;
      return this;
    }

    public Builder setTaskCmd(String taskCmd) {
      this.taskCmd = taskCmd;
      return this;
    }

    public Builder setVenv(String venv) {
      this.venv = venv;
      return this;
    }

    public Builder setShellEnv(Map<String, String> shellEnv) {
      this.shellEnv = shellEnv;
      return this;
    }

    public Builder setAMAddress(String amAddress) {
      this.amAddress = amAddress;
      return this;
    }

    public Builder setTaskExecutorJVMArgs(String jvmArgs) {
      this.jvmArgs = jvmArgs;
      return this;
    }

    public Builder setPsContainerRequest(TensorFlowContainerRequest psRequest) {
      this.psContainerRequest = psRequest;
      return this;
    }

    public Builder setWorkerContainerRequest(TensorFlowContainerRequest workerRequest) {
      this.workerContainerRequest = workerRequest;
      return this;
    }
  }

  /**
   * A TFTask represents a task job executed in the workers.
   */
  public class TFTask {
    private final String jobName;
    private final String jobIndex;
    private String host;
    private int port = -1;

    /**
     * The container the task is running in. Set once a container has been allocated for the task.
     */
    private Container container;

    int exitStatus = -1;

    /**
     * Set to true when exit status is set.
     */
    boolean completed = false;

    public String getJobName() {
      return jobName;
    }

    public String getJobIndex() {
      return jobIndex;
    }

    public String getHost() {
      return host;
    }

    public Container getContainer() {
      return container;
    }

    public void setContainer(Container container) {
      this.container = container;
    }

    public boolean isCompleted() {
      return completed;
    }

    String getHostPort() {
      return String.format("%s:%d", host, port < 0 ? 0 : port);
    }

    public void setHostPort(String hostPort) {
      this.host = hostPort.split(":")[0];
      this.port = Integer.parseInt(hostPort.split(":")[1]);
    }

    int getExitStatus() {
      return exitStatus;
    }

    void setExitStatus(int status) {
      this.completed = true;
      this.exitStatus = status;
    }

    /**
     * Returns a {@link TaskUrl} containing the HTTP URL for the task.
     */
    public TaskUrl getTaskUrl() {
      if (container == null) {
        return null;
      }
      return new TaskUrl(jobName, jobIndex, Utils.constructContainerUrl(container));
    }

    TFTask(String jobName, String jobIndex) {
      this.jobName = jobName;
      this.jobIndex = jobIndex;
    }

    public void addContainer(Container container) {
      setContainer(container);
      containerIdMap.put(container.getId(), this);
    }

    /**
     * Combination of jobName and Index.
     * @return Id
     */
    public String getId() {
      return this.jobName + ":" + this.jobIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TFTask tfTask = (TFTask) o;
      return Objects.equals(jobName, tfTask.jobName) && Objects.equals(jobIndex, tfTask.jobIndex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobName, jobIndex);
    }
  }

  public TFTask getTask(String taskId) {
    try {
      String[] tSplit = taskId.split(":");
      return jobTasks.get(tSplit[0])[Integer.parseInt(tSplit[1])];
    } catch (Exception e) {
      return null;
    }
  }
}
