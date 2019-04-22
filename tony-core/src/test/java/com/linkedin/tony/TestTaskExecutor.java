/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;


public class TestTaskExecutor {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTaskExecutorConfShouldThrowException() throws Exception {
    TaskExecutor taskExecutor = new TaskExecutor();
    Configuration tonyConf = new Configuration(false);
    tonyConf.setInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS, 2000);
    File confFile = new File(System.getProperty("user.dir"), Constants.TONY_FINAL_XML);
    try (OutputStream os = new FileOutputStream(confFile)) {
      tonyConf.writeXml(os);
    }
    if (!confFile.delete()) {
      throw new RuntimeException("Failed to delete conf file");
    }
    // Should throw exception since we didn't set up Task Command.
    taskExecutor.initConfigs();
  }

}
