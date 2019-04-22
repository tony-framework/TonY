/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import com.google.common.base.Preconditions;
import com.linkedin.tony.Constants;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.impl.TaskStatus;
import com.linkedin.tony.util.Utils;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static com.linkedin.tony.Constants.CHIEF_JOB_NAME;
import static com.linkedin.tony.Constants.PS_JOB_NAME;
import static com.linkedin.tony.Constants.WORKER_JOB_NAME;


/**
 * Represents a Tony session.
 */
public class TonySession {
  private static final Log LOG = LogFactory.getLog(TonySession.class);
  private Configuration tonyConf;

  private Map<String, TensorFlowContainerRequest> containerRequests;

  // sessionId to distinguish different sessions. Currently used to distinguish
  // failed session and new session.
  public int sessionId = 0;

  // A map from task name to an array of TFTasks with that name.
  private Map<String, TonyTask[]> jobTasks = new ConcurrentHashMap<>();

  private FinalApplicationStatus sessionFinalStatus = FinalApplicationStatus.UNDEFINED;
  private String sessionFinalMessage = null;
  private String jvmArgs;

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
        .append(" com.linkedin.tony.TaskExecutor");
    return cmd.toString();
  }

  private Map<ContainerId, TonyTask> containerIdMap = new HashMap<>();

  public TonySession() {
  }

  private TonySession(Builder builder) {
    this.containerRequests = Utils.parseContainerRequests(builder.tonyConf);
    this.jvmArgs = builder.jvmArgs;
    this.tonyConf = builder.tonyConf;

    for (Map.Entry<String, TensorFlowContainerRequest> entry : containerRequests.entrySet()) {
      jobTasks.put(entry.getKey(), new TonyTask[entry.getValue().getNumInstances()]);
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
    long tonyConfTimestamp = Long.parseLong(env.get(Constants.TONY_CONF_PREFIX + Constants.TIMESTAMP_SUFFIX));
    long tonyConfLength = Long.parseLong(env.get(Constants.TONY_CONF_PREFIX + Constants.LENGTH_SUFFIX));

    LocalResource tonyConfResource =
        LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(URI.create(tonyConfPath)),
            LocalResourceType.FILE, LocalResourceVisibility.PRIVATE,
            tonyConfLength, tonyConfTimestamp);
    localResources.put(Constants.TONY_FINAL_XML, tonyConfResource);

    try {
      if (hdfsClasspathDir != null) {
        FileSystem fs = FileSystem.get(new URI(hdfsClasspathDir), hdfsConf);
        Utils.addResource(hdfsClasspathDir, localResources, fs);
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
        if (task == null || task.getTaskInfo() == null) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Get a TensorFlow task that hasn't been scheduled.
   * In the absence of allocationRequestId, we are relying on the fact that each tensorflow job will
   * have a distinct priority (Ensured in {@link Utils#parseContainerRequests(Configuration)}).
   * @param priority the priority of the allocated container
   * @return task to be assigned to this allocation
   */
  public synchronized TonyTask getAndInitMatchingTaskByPriority(int priority) {
    for (Map.Entry<String, TensorFlowContainerRequest> entry : containerRequests.entrySet()) {
      String jobName = entry.getKey();
      if (entry.getValue().getPriority() != priority) {
        LOG.debug("Ignoring jobname {" + jobName + "} as priority doesn't match");
        continue;
      }
      TonyTask[] tasks = jobTasks.get(jobName);
      for (int i = 0; i < tasks.length; i++) {
        if (tasks[i] == null) {
          tasks[i] = new TonyTask(jobName, String.valueOf(i), sessionId);
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
          task.getTaskInfo().setState(TaskStatus.FAILED);
          setFinalStatus(FinalApplicationStatus.FAILED, "Exit status: " + exitCode);
        } else {
          task.getTaskInfo().setState(TaskStatus.SUCCEEDED);
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
          "At least one job task exited with non-zero status, failedCnt=" + failureCount);
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
    for (TonyTask[] tasks : jobTasks.values()) {
      for (TonyTask task : tasks) {
        task.getTaskInfo().setState(TaskStatus.FINISHED);
      }

    }
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
  public boolean isChief(String jobName, String index) {
    return jobName.equals(CHIEF_JOB_NAME) || (!jobTasks.containsKey(CHIEF_JOB_NAME)
        && jobName.equals(WORKER_JOB_NAME) && index.equals("0"));
  }

  public TonyTask getTask(ContainerId containerId) {
    return containerIdMap.get(containerId);
  }

  /**
   * Builder to compose the TonySession class.
   */
  public static class Builder {
    private String jvmArgs;
    private Configuration tonyConf;

    public TonySession build() {
      return new TonySession(this);
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
    private TaskInfo taskInfo;

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
      if (status == 0) {
        taskInfo.setState(TaskStatus.SUCCEEDED);
      } else {
        taskInfo.setState(TaskStatus.FAILED);
      }
      this.completed = true;
      this.exitStatus = status;
    }

    /**
     * Returns a {@link TaskInfo} containing the HTTP URL for the task.
     */
    public TaskInfo getTaskInfo() {
      return taskInfo;
    }

    public void setTaskInfo(Container container) {
      taskInfo = new TaskInfo(jobName, taskIndex, Utils.constructContainerUrl(container));
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
