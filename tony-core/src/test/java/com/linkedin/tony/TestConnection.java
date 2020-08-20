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
  private static void assertConnectionIsOpen(Connection connection) {
    if (connection instanceof NettyConnection) {
      assertFalse(((NettyConnection) connection).eventLoopGroup.isShutdown());
      assertTrue(((NettyConnection) connection).future.channel().isOpen());
    } else if (connection instanceof ServerSocketConnection) {
      assertFalse((((ServerSocketConnection) connection).serverSocket).isClosed());
    }
  }

  /**
   * An util method asserting given connection is closed
   * @param connection
   */
  private static void assertConnectionIsClosed(Connection connection) {
    if (connection instanceof NettyConnection) {
      assertTrue(((NettyConnection) connection).eventLoopGroup.isShutdown());
      assertFalse(((NettyConnection) connection).future.channel().isOpen());
    } else if (connection instanceof ServerSocketConnection) {
      assertTrue((((ServerSocketConnection) connection).serverSocket).isClosed());
    }
  }

  /**
   * Tests {@link NettyConnection#create()} works
   */
  @Test
  public void testCreateConnection() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    Connection connWithoutPortReuse = null;
    Connection connWithPortReuse = null;
    try {
      // Verify createConnection WITHOUT port reuse works
      connWithoutPortReuse = ServerSocketConnection.create();
      assertConnectionIsOpen(connWithoutPortReuse);
      // Verify createConnection WITH port reuse works
      connWithPortReuse = NettyConnection.create();
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
  public void testShutDownConnection() {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    NettyConnection nettyConn = null;
    try {
      nettyConn = NettyConnection.create();
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
    ServerSocketConnection connWithoutPortReuse = null;
    NettyConnection connWithPortReuse = null;
    try {
      connWithoutPortReuse = ServerSocketConnection.create();
      // Ensure this is an valid connection
      assertConnectionIsOpen(connWithoutPortReuse);
      int port = connWithoutPortReuse.getPort();

      // Expect connection creation with same port should throw exception
      try {
        connWithPortReuse = NettyConnection.create(port);
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
   * Tests {@link NettyConnection#getPort()}
   */
  @Test
  public void testPortReusableConnectionGetPort() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    ServerSocketConnection conn1 = null;
    int port = -1;
    try {
      conn1 = ServerSocketConnection.create();
      port = conn1.getPort();
    } finally {
      if (conn1 != null) {
        conn1.close();
      }
    }

    NettyConnection conn2 = null;
    try {
      conn2 = NettyConnection.create(port);
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
  public void testCreateServerSocketSuccessWithPortReuse() throws BindException,
      InterruptedException {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    // Given one established connection with port reuse, creating another connection with same
    // port should work.
    NettyConnection nettyConn1 = null;
    NettyConnection nettyConn2 = null;

    try {
      nettyConn1 = NettyConnection.create();
      int port = nettyConn1.getPort();
      nettyConn2 = NettyConnection.create(port);

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
