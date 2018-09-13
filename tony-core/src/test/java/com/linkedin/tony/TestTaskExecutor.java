/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;


public class TestTaskExecutor {
  private String[] args;

  @BeforeTest
  public void setup() {
    List<String> listArgs = new ArrayList<>();
    listArgs.add("-am_address");
    listArgs.add("localhost:1234");
    listArgs.add("-task_command");
    listArgs.add("'sleep 5'");
    listArgs.add("-venv");
    listArgs.add("venv.zip");
    args = listArgs.toArray(new String[listArgs.size()]);
  }

  @Test
  public void testTaskExecutorConf() throws Exception {
    TaskExecutor taskExecutor = new TaskExecutor();
    Configuration tonyConf = new Configuration(false);
    tonyConf.setInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS, 2000);
    File confFile = new File(System.getProperty("user.dir"), Constants.TONY_XML);
    try (OutputStream os = new FileOutputStream(confFile)) {
      tonyConf.writeXml(os);
    }
    taskExecutor.init(args);
    assertEquals(2000, taskExecutor.tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS));
    confFile.delete();
  }
}
