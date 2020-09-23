/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.impl.TaskStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class TestTonyClient {

  @Test
  public void testTonyClientInit() throws Exception {
    String[] args = { "-conf", TonyConfigurationKeys.AM_GPUS + "=1" };
    TonyClient client = new TonyClient();
    client.init(args);
    assertEquals(1, client.getTonyConf().getInt(TonyConfigurationKeys.AM_GPUS, TonyConfigurationKeys.DEFAULT_AM_GPUS));
  }

  @Test
  public void testHdfsClasspathNoScheme() throws IOException, ParseException {
    String hdfsConfPath = getClass().getClassLoader().getResource("test-core-site.xml").getPath();
    String[] args = { "-conf", TonyConfigurationKeys.HDFS_CONF_LOCATION + "=" + hdfsConfPath,
        "-hdfs_classpath", "/my/hdfs/path" };
    TonyClient client = new TonyClient();
    client.init(args);
    String containerResources = client.getTonyConf().get(TonyConfigurationKeys.getContainerResourcesKey());
    assertEquals(containerResources, "hdfs://example.com:9000/my/hdfs/path");
  }

  @Test
  public void testValidateTonyConfValidConf() {
    Configuration conf = new Configuration();
    conf.setInt("tony.foo.instances", 2);
    conf.setInt("tony.bar.instances", 2);
    TonyClient.validateTonyConf(conf);
  }

  @Test
  public void testValidateTonyConfZeroInstances() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.MAX_TOTAL_INSTANCES, 0);
    TonyClient.validateTonyConf(conf);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testValidateTonyConfTooManyTotalInstances() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.MAX_TOTAL_INSTANCES, 3);
    conf.setInt("tony.foo.instances", 2);
    conf.setInt("tony.bar.instances", 2);
    TonyClient.validateTonyConf(conf);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testValidateTonyConfTooManyFooInstances() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.getMaxInstancesKey("foo"), 1);
    conf.setInt("tony.foo.instances", 2);
    TonyClient.validateTonyConf(conf);
  }

  /**
   * 10 GPUs total requested, max is 5. Conf validation should fail.
   */
  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*amount of " + Constants.GPUS + ".*requested exceeds.*")
  public void testValidateTonyConfTooManyGpus() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.GPUS), 1);
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 10);
    conf.setInt(TonyConfigurationKeys.getMaxTotalResourceKey(Constants.GPUS), 5);
    TonyClient.validateTonyConf(conf);
  }

  /**
   * 6 GPUs requested (1 each for the 3 workers and 1 each for the 3 ps), max total is 5. Validation should fail.
   */
  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*amount of " + Constants.GPUS + ".*requested exceeds.*")
  public void testValidateTonyConfTooManyGpusAcrossAllTasks() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.GPUS), 1);
    conf.setInt(TonyConfigurationKeys.getResourceKey(Constants.PS_JOB_NAME, Constants.GPUS), 1);
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 3);
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME), 3);
    conf.setInt(TonyConfigurationKeys.getMaxTotalResourceKey(Constants.GPUS), 5);
    TonyClient.validateTonyConf(conf);
  }

  /**
   * 5 GPUs requested, max total is 10. Validation should pass.
   */
  @Test
  public void testValidateTonyConfRequestedGpusUnderLimit() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.getResourceKey(Constants.WORKER_JOB_NAME, Constants.GPUS), 1);
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 5);
    conf.setInt(TonyConfigurationKeys.getMaxTotalResourceKey(Constants.GPUS), 10);
    TonyClient.validateTonyConf(conf);
  }

  @Test
  public void testTonyApplicatonTags() throws Exception {
    final String applicationTags = "flow_id:1,exec_id:0,proj_name:demo";
    String[] args = { "-conf",
        TonyConfigurationKeys.APPLICATION_TAGS + "=" + applicationTags };
    TonyClient client = new TonyClient();
    client.init(args);
    assertEquals(
        applicationTags,
        client.getTonyConf().get(TonyConfigurationKeys.APPLICATION_TAGS));
  }

  @Test
  public void testSortingTasks() {
    List<TaskInfo> tasks = new ArrayList<>();
    TaskInfo task1 = new TaskInfo("worker", "0", "url");
    task1.setStatus(TaskStatus.RUNNING);
    TaskInfo task2 = new TaskInfo("worker", "1", "url");
    task2.setStatus(TaskStatus.SUCCEEDED);
    TaskInfo task3 = new TaskInfo("worker", "2", "url");
    task3.setStatus(TaskStatus.RUNNING);
    TaskInfo task4 = new TaskInfo("worker", "3", "url");
    task4.setStatus(TaskStatus.FAILED);
    TaskInfo task5 = new TaskInfo("worker", "4", "url");
    task5.setStatus(TaskStatus.RUNNING);
    TaskInfo task6 = new TaskInfo("worker", "5", "url");
    task6.setStatus(TaskStatus.FINISHED);
    TaskInfo task7 = new TaskInfo("ps", "0", "url");
    task7.setStatus(TaskStatus.RUNNING);
    TaskInfo task8 = new TaskInfo("ps", "1", "url");
    task8.setStatus(TaskStatus.FAILED);

    tasks.add(task1);
    tasks.add(task2);
    tasks.add(task3);
    tasks.add(task4);
    tasks.add(task5);
    tasks.add(task6);
    tasks.add(task7);
    tasks.add(task8);

    Collections.shuffle(tasks);
    List<TaskInfo> expected = new ArrayList<>(Arrays.asList(task8, task4, task2, task6, task7,
        task1, task3, task5));

    int i = 0;
    for (TaskInfo task : TonyClient.sortTaskInfo(tasks)) {
      assertEquals(task, expected.get(i++));
    }
  }

  @Test
  public void testMergeTasks() {
    List<TaskInfo> tasks = new ArrayList<>();
    tasks.add(new TaskInfo("ps", "1", "url"));
    tasks.add(new TaskInfo("ps", "2", "url"));
    tasks.add(new TaskInfo("ps", "3", "url"));
    tasks.add(new TaskInfo("worker", "4", "url"));
    tasks.add(new TaskInfo("worker", "5", "url"));
    tasks.add(new TaskInfo("worker", "6", "url"));
    assertEquals(TonyClient.mergeTasks(tasks).trim(), "ps {1, 2, 3} worker {4, 5, 6}");
  }

  @Test
  public void testMergeTasksSingleElement() {
    List<TaskInfo> tasks = new ArrayList<>();
    tasks.add(new TaskInfo("ps", "1", "url"));
    assertEquals(TonyClient.mergeTasks(tasks).trim(), "ps {1}");

    tasks.add(new TaskInfo("worker", "1", "url"));
    assertEquals(TonyClient.mergeTasks(tasks).trim(), "ps {1} worker {1}");
  }
}
