/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.models.JobContainerRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.mockito.Mock;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;


public class TestTaskScheduler {
  TonySession session = mock(TonySession.class);
  AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient = mock(AMRMClientAsync.class);
  Map<String, Map<String, LocalResource>> jobTypeToContainerResources = mock(HashMap.class);
  Map<String, LocalResource> localResources = mock(HashMap.class);
  Configuration conf = mock(Configuration.class);
  TaskScheduler taskScheduler;

  @Mock
  FileSystem fileSystem;


  @BeforeClass
  public void doBeforeClass() {
    taskScheduler = new TaskScheduler(session, amRMClient, localResources, fileSystem, conf, jobTypeToContainerResources);
    doNothing().when(amRMClient).addContainerRequest(any());
    when(jobTypeToContainerResources.put(any(), any())).thenReturn(new HashMap<>());
    when(conf.getStrings(any())).thenReturn(null);
  }

  @Test
  public void testIsDAG() {
    JobContainerRequest dbJob = new JobContainerRequest("db", 1, 0L, 1, 0, 1, "", new ArrayList<>());
    JobContainerRequest workerJob = new JobContainerRequest("worker", 1, 0L, 1, 0, 1, "", Arrays.asList("db", "ps"));
    JobContainerRequest psJob = new JobContainerRequest("ps", 1, 0L, 1, 0, 1, "", Arrays.asList("db"));
    JobContainerRequest dbWriterJob = new JobContainerRequest("dbwriter", 1, 0L, 1, 0, 1, "", new ArrayList<>());
    JobContainerRequest cleanupJob = new JobContainerRequest("cleanup", 1, 0L, 1, 0, 1, "", Arrays.asList("db", "worker"));

    List<JobContainerRequest> requests = Arrays.asList(dbJob, workerJob, psJob, dbWriterJob, cleanupJob);

    boolean actualIsDAG = TaskScheduler.isDAG(requests);
    assertTrue(actualIsDAG);
  }

  @Test
  public void testNotDAG() {
    JobContainerRequest dbJob = new JobContainerRequest("db", 1, 0L, 1, 0, 1, "", Arrays.asList("cleanup"));
    JobContainerRequest workerJob = new JobContainerRequest("worker", 1, 0L, 1, 0, 1, "", Arrays.asList("db", "ps"));
    JobContainerRequest psJob = new JobContainerRequest("ps", 1, 0L, 1, 0, 1, "", Arrays.asList("db"));
    JobContainerRequest dbWriterJob = new JobContainerRequest("dbwriter", 1, 0L, 1, 0, 1, "",  new ArrayList<>());
    JobContainerRequest cleanupJob = new JobContainerRequest("cleanup", 1, 0L, 1, 0, 1, "", Arrays.asList("db", "worker"));

    List<JobContainerRequest> requests = Arrays.asList(dbJob, workerJob, psJob, dbWriterJob, cleanupJob);

    boolean actualIsDAG = TaskScheduler.isDAG(requests);
    assertFalse(actualIsDAG);
  }

  @Test
  public void testScheduleTasks() {
    JobContainerRequest dbJob = new JobContainerRequest("db", 1, 0L, 1, 0, 1, "", new ArrayList<>());
    JobContainerRequest workerJob = new JobContainerRequest("worker", 2, 0L, 1, 0, 1, "", Arrays.asList("db", "ps"));
    JobContainerRequest psJob = new JobContainerRequest("ps", 4, 0L, 1, 0, 1, "", Arrays.asList("db"));
    JobContainerRequest dbWriterJob = new JobContainerRequest("dbwriter", 8, 0L, 1, 0, 1, "",  new ArrayList<>());
    JobContainerRequest cleanupJob = new JobContainerRequest("cleanup", 16, 0L, 1, 0, 1, "", Arrays.asList("db", "worker"));

    List<JobContainerRequest> requests = Arrays.asList(dbJob, workerJob, psJob, dbWriterJob, cleanupJob);

    when(session.getContainersRequests()).thenReturn(requests);

    when(session.getContainerRequestForType("db")).thenReturn(dbJob);
    when(session.getContainerRequestForType("worker")).thenReturn(workerJob);
    when(session.getContainerRequestForType("ps")).thenReturn(psJob);
    when(session.getContainerRequestForType("dbwriter")).thenReturn(dbWriterJob);
    when(session.getContainerRequestForType("cleanup")).thenReturn(cleanupJob);

    taskScheduler.scheduleTasks();

    assertTrue(taskScheduler.checkDependencySatisfied(dbJob));
    assertTrue(taskScheduler.checkDependencySatisfied(dbWriterJob));
    assertFalse(taskScheduler.checkDependencySatisfied(psJob));
    assertFalse(taskScheduler.checkDependencySatisfied(cleanupJob));
    assertFalse(taskScheduler.checkDependencySatisfied(workerJob));

    for (int i = 0; i < dbWriterJob.getNumInstances(); i++) {
      taskScheduler.registerDependencyCompleted("dbwriter");
    }
    assertTrue(taskScheduler.checkDependencySatisfied(dbJob));
    assertTrue(taskScheduler.checkDependencySatisfied(dbWriterJob));
    assertFalse(taskScheduler.checkDependencySatisfied(psJob));
    assertFalse(taskScheduler.checkDependencySatisfied(cleanupJob));
    assertFalse(taskScheduler.checkDependencySatisfied(workerJob));

    for (int i = 0; i < dbJob.getNumInstances(); i++) {
      taskScheduler.registerDependencyCompleted("db");
    }

    assertTrue(taskScheduler.checkDependencySatisfied(dbJob));
    assertTrue(taskScheduler.checkDependencySatisfied(dbWriterJob));
    assertTrue(taskScheduler.checkDependencySatisfied(psJob));
    assertFalse(taskScheduler.checkDependencySatisfied(cleanupJob));
    assertFalse(taskScheduler.checkDependencySatisfied(workerJob));

    // Test ps job not fully finished, should not start workerJob
    for (int i = 0; i < psJob.getNumInstances() - 1; i++) {
      taskScheduler.registerDependencyCompleted("ps");
    }

    assertTrue(taskScheduler.checkDependencySatisfied(dbJob));
    assertTrue(taskScheduler.checkDependencySatisfied(dbWriterJob));
    assertTrue(taskScheduler.checkDependencySatisfied(psJob));
    assertFalse(taskScheduler.checkDependencySatisfied(cleanupJob));
    assertFalse(taskScheduler.checkDependencySatisfied(workerJob));

    taskScheduler.registerDependencyCompleted("ps");

    assertTrue(taskScheduler.checkDependencySatisfied(dbJob));
    assertTrue(taskScheduler.checkDependencySatisfied(dbWriterJob));
    assertTrue(taskScheduler.checkDependencySatisfied(psJob));
    assertFalse(taskScheduler.checkDependencySatisfied(cleanupJob));
    assertTrue(taskScheduler.checkDependencySatisfied(workerJob));

    for (int i = 0; i < workerJob.getNumInstances(); i++) {
      taskScheduler.registerDependencyCompleted("worker");
    }

    assertTrue(taskScheduler.checkDependencySatisfied(dbJob));
    assertTrue(taskScheduler.checkDependencySatisfied(dbWriterJob));
    assertTrue(taskScheduler.checkDependencySatisfied(psJob));
    assertTrue(taskScheduler.checkDependencySatisfied(cleanupJob));
    assertTrue(taskScheduler.checkDependencySatisfied(workerJob));
  }
}
