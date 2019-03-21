/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
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
    conf.setInt(TonyConfigurationKeys.TONY_MAX_TOTAL_INSTANCES, 0);
    TonyClient.validateTonyConf(conf);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testValidateTonyConfTooManyTotalInstances() {
    Configuration conf = new Configuration();
    conf.setInt(TonyConfigurationKeys.TONY_MAX_TOTAL_INSTANCES, 3);
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
   * Since we are switching from passing arguments to ApplicationMaster & TaskExecutor
   * to passing tony configuration file. It is critical to make sure all fields in
   * TonyConfFinal.xml is properly set up.
   * Adding TestTonyE2E is heavy, this function serves as a simplified lightweight
   * place to make sure TonyConfFinal is set correctly.
   */
  @Test
  public void testTonyFinalConf() throws IOException, YarnException, ParseException,
      InterruptedException, URISyntaxException {
    Configuration conf = new Configuration();
    TonyClient client = spy(new TonyClient(conf));
    client.init(new String[]{
        "--executes", "ls",
        "--shell_env", "TEST1=test",
        "--container_env", "TEST2=test",
        "--conf", "tony.application.worker.command=cat"
    });
    // Stub actual app submission logic
    doReturn(true).when(client).monitorApplication();
    doNothing().when(client).submitApplication(any());
    client.start();
    String path = client.processFinalTonyConf();
    Configuration finalConf = new Configuration();
    finalConf.addResource(new Path(path));
    assertEquals(finalConf.get(TonyConfigurationKeys.getContainerExecuteCommandKey()), "ls");
    assertEquals(finalConf.get(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV), "TEST2=test");
    assertEquals(finalConf.get(TonyConfigurationKeys.EXECUTION_ENV), "TEST1=test");
    assertEquals(finalConf.get(TonyConfigurationKeys.getExecuteCommandKey("worker")), "cat");
  }
}
