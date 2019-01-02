/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import com.google.common.base.Preconditions;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.util.Utils;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static com.linkedin.tony.Constants.*;


/**
 * Represents a Tony session.
 */
public class TonySession {
  private static final Log LOG = LogFactory.getLog(TonySession.class);
  private Configuration tonyConf;

  private String taskCmd;
  private String amAddress;
  private Map<String, TensorFlowContainerRequest> containerRequests;

  // sessionId to distinguish different sessions. Currently used to distinguish
  // failed session and new session.
  public int sessionId = 0;

  // A map from task name to an array of TFTasks with that name.
  private Map<String, TonyTask[]> jobTasks = new ConcurrentHashMap<>();

  private FinalApplicationStatus sessionFinalStatus = FinalApplicationStatus.UNDEFINED;
  private String sessionFinalMessage = null;
  private Map<String, String> shellEnv;
  private String jvmArgs;
  private Map<String, Set<Long>> jobTypeToAllocationIds = new HashMap<String, Set<Long>>();

  // If the training has finished. This is used to signal AM to stop waiting for other workers to finish and
  // go straight to the cleaning phase.
  private boolean trainingFinished = false;

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
    for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
      cmd.append(" --shell_env ");
      cmd.append(entry.getKey());
      cmd.append("=");
      cmd.append(entry.getValue());
    }
    return cmd.toString();
  }

  private Map<ContainerId, TonyTask> containerIdMap = new HashMap<>();

  public TonySession() {
  }

  private TonySession(Builder builder) {
    this.taskCmd = builder.taskCmd;
    this.amAddress = builder.amAddress;
    this.containerRequests = Utils.parseContainerRequests(builder.tonyConf);
    this.shellEnv = builder.shellEnv;
    this.jvmArgs = builder.jvmArgs;
    this.tonyConf = builder.tonyConf;

    for (String jobName : containerRequests.keySet()) {
      jobTasks.put(jobName, new TonyTask[containerRequests.get(jobName).getNumInstances()]);
    }
  }

  public Map<String, TonyTask[]> getTonyTasks() {
    return this.jobTasks;
  }


  public boolean isTrainingFinished() {
    return trainingFinished;
  }

  public void setResources(Configuration yarnConf,
                           Configuration hdfsConf,
                           Map<String, LocalResource> localResources,
                           Map<String, String> shellEnv,
                           String hdfsClasspathDir) {

    Map<String, String> env = System.getenv();
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
    for (Map.Entry<String, TonyTask[]> entry : jobTasks.entrySet()) {
      TonyTask[] tasks = entry.getValue();
      for (TonyTask task : tasks) {
        if (task == null) {
          requests.add(getContainerRequestForType(entry.getKey()));
        }
      }
    }
    return requests;
  }

  public TensorFlowContainerRequest getContainerRequestForType(String jobType) {
    return containerRequests.get(jobType);
  }

  public boolean allTasksScheduled() {
    for (TonyTask[] tasks : jobTasks.values()) {
      for (TonyTask task : tasks) {
        if (task == null || task.getTaskUrl() == null) {
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
  public void addAllocationId(String jobName, long allocationRequestId) {
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
  public synchronized TonyTask getAndInitMatchingTask(long allocationRequestId) {
    for (Map.Entry<String, TonyTask[]> entry : jobTasks.entrySet()) {
      String jobName = entry.getKey();
      if (!jobTypeToAllocationIds.get(jobName).contains(allocationRequestId)) {
        continue;
      }
      TonyTask[] tasks = entry.getValue();
      for (int i = 0; i < tasks.length; i++) {
        if (tasks[i] == null) {
          tasks[i] = new TonyTask(jobName, String.valueOf(i), sessionId);
          LOG.info(String.format("Matched job %s with allocationRequestId %d", jobName, allocationRequestId));
          return tasks[i];
        }
      }
    }
    return null;
  }

  public Map<String, List<String>> getClusterSpec() {
    Map<String, List<String>> map = new HashMap<>();

    for (Map.Entry<String, TonyTask[]> entry : jobTasks.entrySet()) {
      String jobName = entry.getKey();
      TonyTask[] tasks = entry.getValue();

      List<String> builder = new ArrayList<>();
      for (TonyTask task : tasks) {
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
    LOG.info(String.format("Job %s:%s exited with %d", jobName, jobIndex, exitCode));
    TonyTask task = getTask(jobName, jobIndex);
    Preconditions.checkNotNull(task);
    TaskType taskType = getTaskType(task);
    task.setExitStatus(exitCode);
    switch (taskType) {
      case TASK_TYPE_CHIEF:
      case TASK_TYPE_PARAMETER_SERVER:
      case TASK_TYPE_OTHERS:
        // If the chief worker failed[chief or worker 0], short circuit and stop the training. Note that even though other
        // worker failures will also fail the job but we don't short circuit the training because the training can still
        // continue, while if chief worker is dead, a TensorFlow training would hang.
        // Also note that, we only short circuit when the chief worker failed, not finished.
        if (exitCode != 0) {
          if (isChief(jobName, jobIndex)) {
            trainingFinished = true;
          }
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
    if (getFinalStatus() == FinalApplicationStatus.FAILED) {
      return;
    }
    for (Map.Entry<String, TonyTask[]> entry : jobTasks.entrySet()) {
      String jobName = entry.getKey();
      TonyTask[] tasks = entry.getValue();

      // If the job type is not tracked, continue.
      if (!Utils.isJobTypeTracked(jobName, tonyConf)) {
        continue;
      }

      for (TonyTask task : tasks) {
        if (task == null) {
          String msg = "Job is null, this should not happen.";
          LOG.error(msg);
          setFinalStatus(FinalApplicationStatus.FAILED, msg);
          return;
        }
        boolean isCompleted = task.isCompleted();
        if (!isCompleted) {
          String msg = "Job " + task.jobName + " at index: " + task.taskIndex + " haven't finished yet.";
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

  private TaskType getTaskType(TonyTask task) {
    TaskType type;
    String jobName = task.getJobName();
    if (jobName.equals(PS_JOB_NAME)) {
      type = TaskType.TASK_TYPE_PARAMETER_SERVER;
    } else {
      type = TaskType.TASK_TYPE_OTHERS;
    }
    return type;
  }

  private TonyTask getTask(String jobName, String taskIndex) {
    for (Map.Entry<String, TonyTask[]> entry : jobTasks.entrySet()) {
      TonyTask[] tasks = entry.getValue();
      for (TonyTask task : tasks) {
        String job = task.getJobName();
        String index = task.getTaskIndex();
        if (job.equals(jobName) && index.equals(taskIndex)) {
          return task;
        }
      }
    }
    return null;
  }

  /**
   * Returns true if the job is "chief" or if there is no "chief" job and ("worker", "0") is passed in.
   */
  private boolean isChief(String jobName, String index) {
    return jobName.equals(CHIEF_JOB_NAME) || (!jobTasks.containsKey(CHIEF_JOB_NAME) &&
        jobName.equals(WORKER_JOB_NAME) && index.equals("0"));
  }

  public TonyTask getTask(ContainerId containerId) {
    return containerIdMap.get(containerId);
  }

  /**
   * Builder to compose the TonySession class.
   */
  public static class Builder {
    private String taskCmd;
    private Map<String, String> shellEnv;
    private String amAddress;
    private String jvmArgs;
    private Configuration tonyConf;

    public TonySession build() {
      return new TonySession(this);
    }

    public Builder setTaskCmd(String taskCmd) {
      this.taskCmd = taskCmd;
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

    public Builder setTonyConf(Configuration tonyConf) {
      this.tonyConf = tonyConf;
      return this;
    }

  }

  /**
   * A TonyTask represents a task job executed in the workers.
   */
  public class TonyTask {
    private final String jobName;
    private final String taskIndex;
    private final int sessionId;
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

    public int getSessionId() {
      return sessionId;
    }

    public String getTaskIndex() {
      return taskIndex;
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
      return new TaskUrl(jobName, taskIndex, Utils.constructContainerUrl(container));
    }

    TonyTask(String jobName, String taskIndex, int sessionId) {
      this.jobName = jobName;
      this.taskIndex = taskIndex;
      this.sessionId = sessionId;
    }

    public void addContainer(Container container) {
      setContainer(container);
      containerIdMap.put(container.getId(), this);
    }

    /**
     * Combination of jobName and taskIndex.
     * @return Id
     */
    public String getId() {
      return this.jobName + ":" + this.taskIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TonyTask tonyTask = (TonyTask) o;
      return Objects.equals(jobName, tonyTask.jobName) && Objects.equals(taskIndex, tonyTask.taskIndex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobName, taskIndex);
    }
  }

  public TonyTask getTask(String taskId) {
    try {
      String[] tSplit = taskId.split(":");
      return jobTasks.get(tSplit[0])[Integer.parseInt(tSplit[1])];
    } catch (Exception e) {
      return null;
    }
  }
}
