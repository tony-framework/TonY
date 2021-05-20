/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTonySession {
  @Test
  public void testTaskAccounting() {
    Configuration tonyConf = new Configuration(false);
    tonyConf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME), 1);
    tonyConf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 2);

    TonySession session = new TonySession.Builder().setTonyConf(tonyConf).build();
    int psPriority = session.getContainerRequestForType(Constants.PS_JOB_NAME).getPriority();
    int workerPriority = session.getContainerRequestForType(Constants.WORKER_JOB_NAME).getPriority();
    session.getAndInitMatchingTaskByPriority(psPriority).setTaskInfo(new ContainerPBImpl());
    // Need to call twice because there are 2 workers.
    session.getAndInitMatchingTaskByPriority(workerPriority).setTaskInfo(new ContainerPBImpl());
    session.getAndInitMatchingTaskByPriority(workerPriority).setTaskInfo(new ContainerPBImpl());
    session.onTaskCompleted(Constants.PS_JOB_NAME, "0", 0, null);
    session.onTaskCompleted(Constants.WORKER_JOB_NAME, "1", 0, null);

    Assert.assertEquals(session.getTotalTasks(), 3);
    Assert.assertEquals(session.getTotalTrackedTasks(), 2);
    Assert.assertEquals(session.getNumCompletedTasks(), 2);
    Assert.assertEquals(session.getNumCompletedTrackedTasks(), 1);
  }

  @Test
  public void testTaskComparable() {
    Configuration tonyConf = new Configuration(false);
    TonySession session = new TonySession.Builder().setTonyConf(tonyConf).build();

    TonySession.TonyTask ps0 = session.buildTonyTask(Constants.PS_JOB_NAME, "0", "localhost");
    TonySession.TonyTask ps1 = session.buildTonyTask(Constants.PS_JOB_NAME, "1", "localhost");
    TonySession.TonyTask worker0 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "0", "localhost");
    TonySession.TonyTask worker1 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "1", "localhost");
    TonySession.TonyTask worker2 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "2", "localhost");
    TonySession.TonyTask samePs1 = session.buildTonyTask(Constants.PS_JOB_NAME, "1", "localhost");
    TonySession.TonyTask sameWorker1 = session.buildTonyTask(Constants.WORKER_JOB_NAME, "1", "localhost");

    Assert.assertTrue(ps0.compareTo(ps1) < 0);
    Assert.assertTrue(worker0.compareTo(worker1) < 0);
    Assert.assertTrue(worker0.compareTo(worker2) < 0);
    Assert.assertTrue(ps0.compareTo(worker0) < 0);
    Assert.assertTrue(ps1.compareTo(worker1) < 0);

    Assert.assertTrue(ps1.compareTo(ps0) > 0);
    Assert.assertTrue(worker1.compareTo(worker0) > 0);
    Assert.assertTrue(worker2.compareTo(worker0) > 0);
    Assert.assertTrue(worker0.compareTo(ps0) > 0);
    Assert.assertTrue(worker1.compareTo(ps1) > 0);

    Assert.assertEquals(worker0.compareTo(worker0), 0);
    Assert.assertEquals(ps0.compareTo(ps0), 0);
    Assert.assertEquals(ps1.compareTo(samePs1), 0);
    Assert.assertEquals(worker1.compareTo(sameWorker1), 0);
  }
}
