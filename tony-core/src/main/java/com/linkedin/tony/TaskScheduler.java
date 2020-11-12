/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.tensorflow.JobContainerRequest;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;


public class TaskScheduler {
  private static final Log LOG = LogFactory.getLog(TaskScheduler.class);
  private TonySession session;
  private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
  private FileSystem resourceFs;
  private Configuration tonyConf;

  // job with dependency -> (dependent job name, number of instances for that job)
  private Map<JobContainerRequest, Map<String, Integer>> taskDependencyMap = new HashMap<>();
  private Map<String, LocalResource> localResources;
  private Map<String, List<AMRMClient.ContainerRequest>> jobTypeToContainerRequestsMap = new HashMap<>();
  private Map<String, Map<String, LocalResource>> jobTypeToContainerResources;

  boolean dependencyCheckPassed = true;

  public TaskScheduler(TonySession session, AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient, Map<String, LocalResource> localResources,
      FileSystem resourceFs, Configuration tonyConf, Map<String, Map<String, LocalResource>> jobTypeToContainerResources) {
    this.session = session;
    this.amRMClient = amRMClient;
    this.localResources = localResources;
    this.resourceFs = resourceFs;
    this.tonyConf = tonyConf;
    this.jobTypeToContainerResources = jobTypeToContainerResources;
  }

  public void scheduleTasks() {
    final List<JobContainerRequest> requests = session.getContainersRequests();

    if (!isDAG(requests)) {
      LOG.error("TonY execution graph does not form a DAG, exiting.");
      session.setFinalStatus(FinalApplicationStatus.FAILED, "App failed due to it not being a DAG.");
      dependencyCheckPassed = false;
      return;
    }

    buildTaskDependencyGraph(requests);

    // start/schedule jobs that have no dependency requirements
    for (JobContainerRequest request : requests) {
      if (checkDependencySatisfied(request)) {
        scheduleJob(request);
      }
    }
  }

  private void buildTaskDependencyGraph(List<JobContainerRequest> requests) {
    for (JobContainerRequest request : requests) {
      for (String dependsOn : request.getDependsOn()) {
        if (!dependsOn.isEmpty()) {
          taskDependencyMap.putIfAbsent(request, new HashMap<>());
          Map<String, Integer> dependenciesForTask = taskDependencyMap.get(request);
          dependenciesForTask.put(dependsOn, session.getContainerRequestForType(dependsOn).getNumInstances());
          taskDependencyMap.put(request, dependenciesForTask);
        }
      }
    }
  }

  @VisibleForTesting
  boolean checkDependencySatisfied(JobContainerRequest request) {
    return taskDependencyMap.get(request) == null || taskDependencyMap.get(request).isEmpty();
  }

  private void scheduleJob(JobContainerRequest request) {
    AMRMClient.ContainerRequest containerAsk = Utils.setupContainerRequestForRM(request);
    String jobName = request.getJobName();
    if (!jobTypeToContainerRequestsMap.containsKey(jobName)) {
      jobTypeToContainerRequestsMap.put(jobName, new ArrayList<>());
      jobTypeToContainerResources.put(jobName, getContainerResources(jobName));
    }
    jobTypeToContainerRequestsMap.get(request.getJobName()).add(containerAsk);
    for (int i = 0; i < request.getNumInstances(); i++) {
      amRMClient.addContainerRequest(containerAsk);
    }
    session.addNumExpectedTask(request.getNumInstances());
  }

  private Map<String, LocalResource> getContainerResources(String jobName) {
    Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
    String[] resources = tonyConf.getStrings(TonyConfigurationKeys.getResourcesKey(jobName));
    Utils.addResources(resources, containerResources, resourceFs);

    // All resources available to all containers
    resources = tonyConf.getStrings(TonyConfigurationKeys.getContainerResourcesKey());
    Utils.addResources(resources, containerResources, resourceFs);
    return containerResources;
  }

  synchronized void registerDependencyCompleted(String jobName) {
    taskDependencyMap.forEach((k, v) -> {
      if (v.containsKey(jobName)) {
        int numContainersLeft = v.get(jobName);
        numContainersLeft--;

        if (numContainersLeft == 0) {
          v.remove(jobName);
        } else {
          v.put(jobName, numContainersLeft);
        }
      }
    });

    Iterator<JobContainerRequest> waitingRequestItr = taskDependencyMap.keySet().iterator();
    while (waitingRequestItr.hasNext()) {
      JobContainerRequest waitingRequest = waitingRequestItr.next();
      if (checkDependencySatisfied((waitingRequest))) {
        waitingRequestItr.remove();
        scheduleJob(waitingRequest);
      }
    }
  }

  static boolean isDAG(final List<JobContainerRequest> containersRequests) {
    Set<JobContainerRequest> visited = new HashSet<>();

    for (JobContainerRequest containerRequest : containersRequests) {
      if (!visited.contains(containerRequest) && !isSubgraphDAG(containerRequest, new ArrayList<>(), containersRequests, visited)) {
        return false;
      }
    }

    return true;
  }

  static boolean isSubgraphDAG(JobContainerRequest node, List<JobContainerRequest> pathTrace,
      final List<JobContainerRequest> containerRequests, Set<JobContainerRequest> visited) {
    if (pathTrace.contains(node)) {
      return false;
    }
    if (visited.contains(node)) {
      return true;
    }

    pathTrace.add(node);
    visited.add(node);

    List<JobContainerRequest> dependencies = containerRequests.stream()
        .filter(x -> node.getDependsOn().contains(x.getJobName()))
        .collect(Collectors.toList());
    for (JobContainerRequest dependency : dependencies) {
      if (!isSubgraphDAG(dependency, pathTrace, containerRequests, visited)) {
        return false;
      }
    }

    pathTrace.remove(node);

    return true;
  }
}
