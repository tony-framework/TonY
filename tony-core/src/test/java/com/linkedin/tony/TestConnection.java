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
    if (connection instanceof PortReusableConnection) {
      assertFalse(((PortReusableConnection) connection).eventLoopGroup.isShutdown());
      assertTrue(((PortReusableConnection) connection).future.channel().isOpen());
    } else if (connection instanceof NonPortReusableConnection) {
      assertFalse((((NonPortReusableConnection) connection).serverSocket).isClosed());
    }
  }

  /**
   * An util method asserting given connection is closed
   * @param connection
   */
  private static void assertConnectionIsClosed(Connection connection) {
    if (connection instanceof PortReusableConnection) {
      assertTrue(((PortReusableConnection) connection).eventLoopGroup.isShutdown());
      assertFalse(((PortReusableConnection) connection).future.channel().isOpen());
    } else if (connection instanceof NonPortReusableConnection) {
      assertTrue((((NonPortReusableConnection) connection).serverSocket).isClosed());
    }
  }

  /**
   * Tests {@link PortReusableConnection#create()} works
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
      connWithoutPortReuse = NonPortReusableConnection.create();
      assertConnectionIsOpen(connWithoutPortReuse);
      // Verify createConnection WITH port reuse works
      connWithPortReuse = PortReusableConnection.create();
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

    PortReusableConnection nettyConn = null;
    try {
      nettyConn = PortReusableConnection.create();
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
    NonPortReusableConnection connWithoutPortReuse = null;
    PortReusableConnection connWithPortReuse = null;
    try {
      connWithoutPortReuse = NonPortReusableConnection.create();
      // Ensure this is an valid connection
      assertConnectionIsOpen(connWithoutPortReuse);
      int port = connWithoutPortReuse.getPort();

      // Expect connection creation with same port should throw exception
      try {
        connWithPortReuse = PortReusableConnection.create(port);
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
   * Tests {@link PortReusableConnection#getPort()}
   */
  @Test
  public void testPortReusableConnectionGetPort() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    NonPortReusableConnection conn1 = null;
    int port = -1;
    try {
      conn1 = NonPortReusableConnection.create();
      port = conn1.getPort();
    } finally {
      if (conn1 != null) {
        conn1.close();
      }
    }

    PortReusableConnection conn2 = null;
    try {
      conn2 = PortReusableConnection.create(port);
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
    PortReusableConnection nettyConn1 = null;
    PortReusableConnection nettyConn2 = null;

    try {
      nettyConn1 = PortReusableConnection.create();
      int port = nettyConn1.getPort();
      nettyConn2 = PortReusableConnection.create(port);

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
