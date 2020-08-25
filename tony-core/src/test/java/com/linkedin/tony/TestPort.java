/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import java.io.IOException;
import java.net.BindException;
import org.apache.commons.lang.SystemUtils;
import org.testng.annotations.Test;

public class TestPort {

  public final static String SKIP_TEST_MESSAGE = "Skip this test since it only runs on Linux "
      + "which has reuse port feature";

  /**
   * An util method asserting given port is open.
   * @param port
   */
  private static void assertPortIsOpen(ServerPort port) {
    if (port instanceof ReusablePort) {
      assertFalse(((ReusablePort) port).eventLoopGroup.isShutdown());
      assertTrue(((ReusablePort) port).future.channel().isOpen());
    } else if (port instanceof EphemeralPort) {
      assertFalse((((EphemeralPort) port).serverSocket).isClosed());
    }
  }

  /**
   * An util method asserting given port is closed
   * @param port
   */
  private static void assertPortIsClosed(ServerPort port) {
    if (port instanceof ReusablePort) {
      assertTrue(((ReusablePort) port).eventLoopGroup.isShutdown());
      assertFalse(((ReusablePort) port).future.channel().isOpen());
    } else if (port instanceof EphemeralPort) {
      assertTrue((((EphemeralPort) port).serverSocket).isClosed());
    }
  }

  /**
   * Tests {@link ReusablePort#create()} works
   */
  @Test
  public void testCreatePort() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }
    ServerPort portWithoutPortReuse = null;
    ServerPort portWithPortReuse = null;
    try {
      // Verify createPort WITHOUT port reuse works
      portWithoutPortReuse = EphemeralPort.create();
      assertPortIsOpen(portWithoutPortReuse);
      // Verify createPort WITH port reuse works
      portWithPortReuse = ReusablePort.create();
      assertPortIsOpen(portWithPortReuse);
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
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
      System.out.println(SKIP_TEST_MESSAGE);
      return;
    }

    ReusablePort reusablePort = null;
    try {
      reusablePort = ReusablePort.create();
      assertPortIsOpen(reusablePort);
      reusablePort.close();
      assertPortIsClosed(reusablePort);
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
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
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
      assertPortIsOpen(reusablePort);
      int port = reusablePort.getPort();

      // Expect port creation with same port should throw exception
      try {
        nonReusablePort = ReusablePort.create(port);
        fail("ReusablePort.create should throw exception when binding to a used port without port "
            + "reuse");
      } catch (BindException exception) {
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

  /**
   * Tests {@link ReusablePort#getPort()}
   */
  @Test
  public void testReusablePortGetPort() throws Exception {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
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

  private static ReusablePort createPortWithPortReuse(int port) throws InterruptedException,
      IOException {
    final EventLoopGroup bossGroup = new EpollEventLoopGroup();
    ServerBootstrap b = new ServerBootstrap();
    ChannelFuture future;
    try {
      b.group(bossGroup)
          .channel(EpollServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
            }
          }).option(EpollChannelOption.SO_REUSEPORT, true)
          .option(ChannelOption.SO_KEEPALIVE, true);

      future = b.bind(port).await();
      if (!future.isSuccess()) {
        throw new BindException("Fail to bind to the port " + port);
      }
      return new ReusablePort(bossGroup, future);
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * Tests port allocation succeed with port reuse when another non-tony process previously binds
   * to a reusable port.
   */
  @Test
  public void testAllocatePortSucceedWithPortReuse() {
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
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
      assertPortIsOpen(reusablePort1);
      assertPortIsOpen(reusablePort2);
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
    // Port reuse feature is only available in Linux, so skip other OSes.
    if (!SystemUtils.IS_OS_LINUX) {
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
