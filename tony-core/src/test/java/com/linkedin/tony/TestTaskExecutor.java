/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

  @Test
  public void testGetExecutionErrorLog() throws IOException {
    TaskExecutor executor = new TaskExecutor();

    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    executor.setContainerLogDir(tmpDir.getAbsolutePath());

    String errorFile = executor.getExecutionStdErrFile();

    executor.setExecutionErrorMsgOutputMaxDepth(20);

    String errorContent = "hello world-1\n"
            + "hello world-2\n"
            + "hello world-3\n"
            + "hello world-4\n"
            + "hello world-5\n"
            + "hello world-6\n"
            + "hello world-7\n"
            + "hello world-8\n"
            + "hello world-9\n"
            + "hello world-10\n";

    FileUtils.writeStringToFile(new File(errorFile), errorContent);

    String captureout = executor.getExecutionErrorLog();
    Assert.assertEquals(captureout, errorContent);

    executor.setExecutionErrorMsgOutputMaxDepth(5);
    captureout = executor.getExecutionErrorLog();
    Assert.assertEquals(5, captureout.split("\n").length);
  }
}
