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
import org.apache.commons.lang.SystemUtils;
import org.testng.annotations.Test;

public class TestConnection {

  public final static String SKIP_TEST_MESSAGE = "Skip this test since it only runs on Linux "
      + "which has reuse port feature";

  /**
   * An util method asserting given connection is open.
   * @param connection
   */
  private static void assertConnectionIsOpen(ServerPort connection) {
    if (connection instanceof ReusablePort) {
      assertFalse(((ReusablePort) connection).eventLoopGroup.isShutdown());
      assertTrue(((ReusablePort) connection).future.channel().isOpen());
    } else if (connection instanceof EphemeralPort) {
      assertFalse((((EphemeralPort) connection).serverSocket).isClosed());
    }
  }

  /**
   * An util method asserting given connection is closed
   * @param connection
   */
  private static void assertConnectionIsClosed(ServerPort connection) {
    if (connection instanceof ReusablePort) {
      assertTrue(((ReusablePort) connection).eventLoopGroup.isShutdown());
      assertFalse(((ReusablePort) connection).future.channel().isOpen());
    } else if (connection instanceof EphemeralPort) {
      assertTrue((((EphemeralPort) connection).serverSocket).isClosed());
    }
  }

  /**
   * Tests {@link ReusablePort#create()} works
   */
  @Test
  public void testCreateConnection() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    ServerPort connWithoutPortReuse = null;
    ServerPort connWithPortReuse = null;
    try {
      // Verify createConnection WITHOUT port reuse works
      connWithoutPortReuse = EphemeralPort.create();
      assertConnectionIsOpen(connWithoutPortReuse);
      // Verify createConnection WITH port reuse works
      connWithPortReuse = ReusablePort.create();
      assertConnectionIsOpen(connWithPortReuse);
    } finally {
      // Make sure connection is always closed
      try {
        if (connWithoutPortReuse != null) {
          connWithoutPortReuse.close();
        }
      } finally {
        if (connWithPortReuse != null) {
          connWithPortReuse.close();
        }
      }
    }
  }


  /**
   * Tests connection can be shutdown successfully
   */
  @Test
  public void testShutDownConnection() throws IOException, InterruptedException {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    ReusablePort nettyConn = null;
    try {
      nettyConn = ReusablePort.create();
      assertConnectionIsOpen(nettyConn);
      nettyConn.close();
      assertConnectionIsClosed(nettyConn);
    } finally {
      // Make sure connection is always closed
      if (nettyConn != null) {
        nettyConn.close();
      }
    }
  }

  /**
   * Tests server socket creation failure when binding to the same port without port reuse
   */
  @Test
  public void testCreateServerSocketFailWithoutPortReuse() throws IOException,
      InterruptedException {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    // Given one established connection without port reuse, creating another connection with same
    // port should fail.
    EphemeralPort connWithoutPortReuse = null;
    ReusablePort connWithPortReuse = null;
    try {
      connWithoutPortReuse = EphemeralPort.create();
      // Ensure this is an valid connection
      assertConnectionIsOpen(connWithoutPortReuse);
      int port = connWithoutPortReuse.getPort();

      // Expect connection creation with same port should throw exception
      try {
        connWithPortReuse = ReusablePort.create(port);
        fail("createConnection should throw exception when binding to a used port without port "
            + "reuse");
      } catch (BindException exception) {
      }
    } finally {
      // Make sure connection is always closed
      try {
        if (connWithoutPortReuse != null) {
          connWithoutPortReuse.close();
        }
      } finally {
        if (connWithPortReuse != null) {
          connWithPortReuse.close();
        }
      }
    }
  }

  /**
   * Tests {@link ReusablePort#getPort()}
   */
  @Test
  public void testPortReusableConnectionGetPort() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    EphemeralPort conn1 = null;
    int port = -1;
    try {
      conn1 = EphemeralPort.create();
      port = conn1.getPort();
    } finally {
      if (conn1 != null) {
        conn1.close();
      }
    }

    ReusablePort conn2 = null;
    try {
      conn2 = ReusablePort.create(port);
      assertEquals(conn2.getPort(), port);
    } finally {
      if (conn2 != null) {
        conn2.close();
      }
    }
  }

  /**
   * Tests server socket creation works with port reuse when binding to the same port
   */
  @Test
  public void testCreateServerSocketSuccessWithPortReuse() throws IOException,
      InterruptedException {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    // Given one established connection with port reuse, creating another connection with same
    // port should work.
    ReusablePort nettyConn1 = null;
    ReusablePort nettyConn2 = null;

    try {
      nettyConn1 = ReusablePort.create();
      int port = nettyConn1.getPort();
      nettyConn2 = ReusablePort.create(port);

      // Assert connections are successfully created
      assertConnectionIsOpen(nettyConn1);
      assertConnectionIsOpen(nettyConn2);
    } finally {
      try {
        if (nettyConn1 != null) {
          nettyConn1.close();
        }
      } finally {
        if (nettyConn2 != null) {
          nettyConn2.close();
        }
      }
    }
  }
}
