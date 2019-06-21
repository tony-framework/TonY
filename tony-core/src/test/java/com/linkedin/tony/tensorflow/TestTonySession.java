/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.tensorflow;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.junit.Assert;
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
    session.onTaskCompleted(Constants.PS_JOB_NAME, "0", 0);
    session.onTaskCompleted(Constants.WORKER_JOB_NAME, "1", 0);

    Assert.assertEquals(3, session.getTotalTasks());
    Assert.assertEquals(2, session.getTotalTrackedTasks());
    Assert.assertEquals(2, session.getNumCompletedTasks());
    Assert.assertEquals(1, session.getNumCompletedTrackedTasks());
  }
}
