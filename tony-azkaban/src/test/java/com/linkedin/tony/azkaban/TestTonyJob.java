/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.azkaban;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.FileIOUtils;
import azkaban.utils.Props;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;


public class TestTonyJob {

  private final Logger log = Logger.getLogger(TestTonyJob.class);

  // Taken from azkaban.test.Utils in azkaban-common.
  private static void initServiceProvider() {
    final Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() { }
    });
    // Because SERVICE_PROVIDER is a singleton and it is shared among many tests,
    // need to reset the state to avoid assertion failures.
    SERVICE_PROVIDER.unsetInjector();

    SERVICE_PROVIDER.setInjector(injector);
  }

  @BeforeTest
  public void setup() {
    initServiceProvider();
  }

  @Test
  public void testMainArguments() {
    final Props jobProps = new Props();
    jobProps.put(TonyJobArg.HDFS_CLASSPATH.azPropName, "hdfs://nn:8020");
    jobProps.put(TonyJob.WORKER_ENV_PREFIX + "E1", "e1");
    jobProps.put(TonyJob.WORKER_ENV_PREFIX + "E2", "e2");

    final TonyJob tonyJob = new TonyJob("test_tony_job", new Props(), jobProps, log) {
      @Override
      public String getWorkingDirectory() {
        return System.getProperty("java.io.tmpdir");
      }
    };
    String args = tonyJob.getMainArguments();
    Assert.assertTrue(args.contains(TonyJobArg.HDFS_CLASSPATH.tonyParamName + " hdfs://nn:8020"));
    Assert.assertTrue(args.contains(TonyJobArg.SHELL_ENV.tonyParamName + " E2=e2"));
    Assert.assertTrue(args.contains(TonyJobArg.SHELL_ENV.tonyParamName + " E1=e1"));
  }

  /**
   * Check if the flow level information is passed to the tony job through configuration.
   */
  @Test
  public void testFlowInfoPropagation() {
    final Props jobProps = new Props();
    jobProps.put(TonyJobArg.HDFS_CLASSPATH.azPropName, "hdfs://nn:8020");
    jobProps.put(CommonJobProperties.PROJECT_NAME, "unit_test");
    jobProps.put(CommonJobProperties.FLOW_ID, "1");
    jobProps.put(CommonJobProperties.EXEC_ID, "0");

    final TonyJob tonyJob = new TonyJob("test_tony_job", new Props(), jobProps, log) {
      @Override
      public String getWorkingDirectory() {
        return System.getProperty("java.io.tmpdir");
      }
    };

    Configuration conf = tonyJob.getTonyJobConf();
    Set<String> values = new HashSet<>(
        conf.getStringCollection(TonyJob.TONY_APPLICATION_TAGS));
    values.remove("");

    Map<String, String> parsedTags = new HashMap<>();
    for (String value : values) {
      String[] pair = value.split(":");
      Assert.assertTrue(pair.length == 2);
      parsedTags.put(pair[0], pair[1]);
    }

    Assert.assertEquals(parsedTags.get(CommonJobProperties.EXEC_ID), "0");
    Assert.assertEquals(parsedTags.get(CommonJobProperties.FLOW_ID), "1");
    Assert.assertEquals(parsedTags.get(CommonJobProperties.PROJECT_NAME), "unit_test");
  }

  @Test
  public void testClassPaths() {
    final Props sysProps = new Props();
    final Props jobProps = new Props();
    jobProps.put(TonyJob.WORKING_DIR, FilenameUtils.getFullPath(FileIOUtils.getSourcePathFromClass(Props.class)));
    sysProps.put("jobtype.classpath", "123,456,789");
    sysProps.put("plugin.dir", "Plugins");
    final TonyJob tonyJob = new TonyJob("test_tony_job_class_path", sysProps, jobProps, log) {
      @Override
      public String getWorkingDirectory() {
        return System.getProperty("java.io.tmpdir");
      }
    };
    List<String> paths = tonyJob.getClassPaths();
    int counter = 0;
    for (String path : paths) {
      if (path.contains("Plugins/123") || path.contains("Plugins/456") || path.contains("Plugins/789")) {
        counter += 1;
      }
    }
    Assert.assertTrue(counter == 3);
    Assert.assertTrue(paths.contains(new File(tonyJob.getWorkingDirectory(), "_tony-conf-test_tony_job_class_path").toString()));
  }
}
