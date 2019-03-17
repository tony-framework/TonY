/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

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
}
