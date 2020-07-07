/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import org.testng.Assert;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
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

  private int getAvailablePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    assert serverSocket.isClosed();
    return port;
  }

  private Path getPortFile(int port) {
    return TaskExecutor.PORT_FILE_DIR.resolve(TaskExecutor.PORT_FILE_PREFIX + port);
  }

  @Test
  public void testPortRangeIsValid() {
    Assert.assertEquals(TaskExecutor.PORT_RANGE.getMinimum().intValue(), 1);
    Assert.assertEquals(TaskExecutor.PORT_RANGE.getMaximum().intValue(), 65535);
  }

  @Test
  public void testCreateServerSocketSuccess() throws IOException {
    TaskExecutor taskExecutor = new TaskExecutor();
    int port = this.getAvailablePort();
    ServerSocket serverSocket = taskExecutor.createServerSocket(port);
    Path portFilePath = null;
    try {
      Assert.assertNotEquals(null, serverSocket);
      Assert.assertEquals(port, serverSocket.getLocalPort());
      portFilePath = getPortFile(serverSocket.getLocalPort());
      Assert.assertTrue(Files.exists(portFilePath));
    } finally {
      Files.deleteIfExists(portFilePath);
      serverSocket.close();
    }
  }

  @Test
  public void testCreateServerSocketFailWithDuplicatePortBind() throws IOException {
    TaskExecutor taskExecutor = new TaskExecutor();
    int port = this.getAvailablePort();
    ServerSocket serverSocket = taskExecutor.createServerSocket(port);
    try {
      ServerSocket anotherSocket = taskExecutor.createServerSocket(port);
      Assert.assertNull(anotherSocket);
    } finally {
      Files.deleteIfExists(getPortFile(serverSocket.getLocalPort()));
      serverSocket.close();
    }
  }

  @Test
  public void testCreateServerSocketFailWithExistingPortFile() throws IOException {
    TaskExecutor taskExecutor = new TaskExecutor();
    int port = this.getAvailablePort();
    ServerSocket serverSocket = taskExecutor.createServerSocket(port);
    serverSocket.close();
    Assert.assertTrue(Files.exists(getPortFile(serverSocket.getLocalPort())));
    try {
      ServerSocket anotherSocket = taskExecutor.createServerSocket(port);
      Assert.assertNull(anotherSocket);
    } finally {
      Files.deleteIfExists(getPortFile(serverSocket.getLocalPort()));
    }
  }

}
