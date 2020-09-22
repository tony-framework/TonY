/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.net.BindException;
import java.time.Duration;
import org.apache.commons.lang.SystemUtils;
import org.testng.annotations.Test;

public class TestPortAllocation {

  public final static String SKIP_TEST_MESSAGE = "Skip this test since it only runs on Linux "
      + "which has reuse port feature";

  /**
   * Tests {@link ReusablePort#create()} works
   */
  @Test
  public void testCreatePort() throws Exception {
    // Port reuse feature is not available in Windows
    if (SystemUtils.IS_OS_WINDOWS) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    ServerPort portWithoutPortReuse = null;
    ServerPort portWithPortReuse = null;
    try {
      // Verify createPort WITHOUT port reuse works
      portWithoutPortReuse = EphemeralPort.create();
      assertPortIsReserved(portWithoutPortReuse.getPort());
      // Verify createPort WITH port reuse works
      portWithPortReuse = ReusablePort.create();
      assertPortIsReserved(portWithPortReuse.getPort());
    } finally {
      // Make sure port is always closed
      try {
        if (portWithoutPortReuse != null) {
          portWithoutPortReuse.close();
        }
      } finally {
        if (portWithPortReuse != null) {
          portWithPortReuse.close();
        }
      }
    }
  }


  /**
   * Tests port can be shutdown successfully
   */
  @Test
  public void testShutDownPort() throws IOException, InterruptedException {
    // Port reuse feature is not available in Windows
    if (SystemUtils.IS_OS_WINDOWS) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    ReusablePort reusablePort = null;
    try {
      reusablePort = ReusablePort.create();
      assertPortIsReserved(reusablePort.getPort());
      reusablePort.close();
      assertPortIsReleased(reusablePort.getPort());
    } finally {
      // Make sure port is always closed
      if (reusablePort != null) {
        reusablePort.close();
      }
    }
  }

  /**
   * Tests port allocation failure when binding to the same port without port reuse
   */
  @Test
  public void testAllocatePortFailWithoutPortReuse() throws IOException,
      InterruptedException {
    // Port reuse feature is not available in Windows
    if (SystemUtils.IS_OS_WINDOWS) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    // Given one established port without port reuse, creating another port with same
    // port should fail.
    EphemeralPort reusablePort = null;
    ReusablePort nonReusablePort = null;
    try {
      reusablePort = EphemeralPort.create();
      // Ensure this is a valid port
      assertPortIsReserved(reusablePort.getPort());
      int port = reusablePort.getPort();

      // Expect port creation with same port should throw exception
      try {
        nonReusablePort = ReusablePort.create(port);
        fail("ReusablePort.create should throw exception when binding to a used port without port "
            + "reuse");
      } catch (Exception exception) {
      }
    } finally {
      // Make sure port is always closed
      try {
        if (reusablePort != null) {
          reusablePort.close();
        }
      } finally {
        if (nonReusablePort != null) {
          nonReusablePort.close();
        }
      }
    }
  }

  private void assertPortIsReserved(int port) throws IOException {
    assertFalse(ReusablePort.isPortAvailable(port));
  }

  private void assertPortIsReleased(int port) throws IOException {
    assertTrue(ReusablePort.isPortAvailable(port));
  }

  /**
   * Tests {@link ReusablePort#getPort()}
   */
  @Test
  public void testReusablePortGetPort() throws Exception {
    // Port reuse feature is not available in Windows
    if (SystemUtils.IS_OS_WINDOWS) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    EphemeralPort port1 = null;
    int port = -1;
    try {
      port1 = EphemeralPort.create();
      port = port1.getPort();
    } finally {
      if (port1 != null) {
        port1.close();
      }
    }

    ReusablePort port2 = null;
    try {
      port2 = ReusablePort.create(port);
      assertEquals(port2.getPort(), port);
    } finally {
      if (port2 != null) {
        port2.close();
      }
    }
  }

  /**
   * A method mocking tensorflow process reserves the port with port reuse.
   */
  private static ReusablePort createPortWithPortReuse(int port) throws IOException {
    String bindSocket = String.format("python %s -p %s -d %s",
        ReusablePort.RESERVE_PORT_SCRIPT_PATH, port, Duration.ofHours(1).getSeconds());

    ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", bindSocket);
    taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    if (ReusablePort.isPortAvailable(port)) {
      System.out.println("Starting process " + bindSocket);
      Process taskProcess = taskProcessBuilder.start();
      return new ReusablePort(taskProcess, port);
    } else {
      System.out.println("Port " + port + " is no longer available.");
      throw new BindException("Fail to bind to the port " + port);
    }
  }

  /**
   * Tests port allocation succeed with port reuse when another non-tony process previously binds
   * to a reusable port.
   */
  @Test
  public void testAllocatePortSucceedWithPortReuse() throws InterruptedException {
    // Port reuse feature is not available in Windows
    if (SystemUtils.IS_OS_WINDOWS) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    ReusablePort reusablePort1 = null;
    ReusablePort reusablePort2 = null;

    try {
      // Tony reserves a port with port reuse
      reusablePort1 = ReusablePort.create();
      int port = reusablePort1.getPort();

      // Mock another non-tony process creates the same port with port reuse.
      // In real-world case, this should be underlying tensorflow process. But for convenience
      // purpose, we use netty to create a reusable port.
      reusablePort2 = createPortWithPortReuse(port);

      // Assert ports are successfully created
      assertPortIsReserved(reusablePort1.getPort());
      assertPortIsReserved(reusablePort2.getPort());
    } catch (Exception e) {
    } finally {
      try {
        if (reusablePort1 != null) {
          reusablePort1.close();
        }
      } finally {
        if (reusablePort2 != null) {
          reusablePort2.close();
        }
      }
    }
  }

  /**
   * Tests port allocation fails with port reuse when another tony process binds to the
   * same port.
   */
  @Test
  public void testAllocatePortFailWithPortReuse() {
    // Port reuse feature is not available in Windows
    if (SystemUtils.IS_OS_WINDOWS) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    ReusablePort reusablePort1 = null;
    ReusablePort reusablePort2 = null;

    try {
      // Tony creates a reusable port
      reusablePort1 = ReusablePort.create();
      int port = reusablePort1.getPort();
      // Mock another Tony process creates the same reusable port and it should fail
      reusablePort2 = ReusablePort.create(port);

      fail("ReusablePort.create should throw exception when binding to a used port");
    } catch (Exception e) {
    } finally {
      try {
        if (reusablePort1 != null) {
          reusablePort1.close();
        }
      } finally {
        if (reusablePort2 != null) {
          reusablePort2.close();
        }
      }
    }
  }
}