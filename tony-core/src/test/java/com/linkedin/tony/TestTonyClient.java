/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

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
}
