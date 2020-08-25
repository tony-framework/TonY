/*
 * Copyright 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class encapsulates netty objects related to an established port which enables SO_REUSEPORT.
 * See <a href="https://lwn.net/Articles/542629/">https://lwn.net/Articles/542629/</a> about
 * SO_REUSEPORT. It only works with Linux platform since EpollEventLoopGroup used in
 * {@link ReusablePort#create(int)} is not supported via other platforms. See
 * <a href="https://netty.io/4.0/api/io/netty/channel/epoll/EpollEventLoopGroup.html">
 *   https://netty.io/4.0/api/io/netty/channel/epoll/EpollEventLoopGroup.html</a>.
 */
final class ReusablePort extends ServerPort {
  private static final Log LOG = LogFactory.getLog(ReusablePort.class);
  final EventLoopGroup eventLoopGroup;
  final ChannelFuture future;
  private ReusablePort(EventLoopGroup loopGroup, ChannelFuture future) {
    this.eventLoopGroup = loopGroup;
    this.future = future;
  }

  private static void close(EventLoopGroup loopGroup, ChannelFuture future) {
    if (future != null && future.channel().isOpen()) {
      future.channel().close().awaitUninterruptibly();
    }

    if (loopGroup != null && !loopGroup.isShutdown()) {
      loopGroup.shutdownGracefully().awaitUninterruptibly();
    }
  }

  /**
   * Closes the port.
   */
  @Override
  public void close() {
    ReusablePort.close(this.eventLoopGroup, this.future);
  }

  /**
   * @return the binding port associated with the connection
   */
  @Override
  int getPort() {
    InetSocketAddress socketAddress =
        (InetSocketAddress) this.future.channel().localAddress();
    return socketAddress.getPort();
  }

  private static boolean isPortAvailable(int port) throws IOException {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
      return true;
    } catch (Exception e) {
      LOG.info(e);
      return false;
    } finally {
      if (serverSocket != null) {
        serverSocket.close();
      }
    }
  }

  private static int getAvailablePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }

  /**
   * Creates a binding port with SO_REUSEPORT.
   * See <a href="https://lwn.net/Articles/542629/">https://lwn.net/Articles/542629/</a> about
   * SO_REUSEPORT.
   * @return the created port
   */
  static ReusablePort create() throws IOException, InterruptedException {
    ReusablePort reusablePort;
    final int portBindingRetry = 5;
    for (int i = 0; i < portBindingRetry; i++) {
      try {
        reusablePort = create(getAvailablePort());
        return reusablePort;
      } catch (BindException ex) {
        LOG.info("port binding attempt " + (i + 1) + " failed.");
      }
    }

    throw new BindException("Unable to bind port after " + portBindingRetry + " attempt(s).");
  }

  /**
   * Creates a binding port with netty library which has built-in port reuse support.
   * <p>port reuse feature is detailed in:
   * <a href="https://lwn.net/Articles/542629/">https://lwn.net/Articles/542629/</a>
   * </p>
   *
   * @param port the port to bind to, cannot be 0 to pick a random port. Since another tony
   *             executor can bind to the same port when port 0 and SO_REUSEPORT are used together.
   * @return the binding port
   * @throws BindException if fails to bind to any port
   * @throws InterruptedException if the thread waiting for incoming connection is interrupted
   */
  @VisibleForTesting
  static ReusablePort create(int port) throws InterruptedException, IOException {
    // Why creating connection with port reuse using netty instead of native socket library
    //(https://docs.oracle.com/javase/8/docs/api/java/net/Socket.html)?
    // - Tony's default Java version is 8 and port reuse feature is only available in Java 9+:
    //   https://docs.oracle.com/javase/9/docs/api/java/net/StandardSocketOptions.html#SO_REUSEPORT.
    //
    // Why not upgrading Tony to Java 9+ given port reuse is supported in Java 9+?
    // - In Linkedin, as of now(2020/08), only Java 8 and 11 are officially supported, but Java 11
    //   introduces incompatibility with Play version tony-portal
    //   (https://github.com/linkedin/TonY/tree/master/tony-portal) is using. Upgrading Play to a
    //   Java 11-compatible version requires non-trivial amount of effort.

    Preconditions.checkArgument(port > 0, "Port must > 0.");
    final EventLoopGroup bossGroup = new EpollEventLoopGroup();
    ServerBootstrap b = new ServerBootstrap();
    ChannelFuture future = null;

    try {
      b.group(bossGroup)
          .channel(EpollServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
            }
          }).option(EpollChannelOption.SO_REUSEPORT, true)
          .option(ChannelOption.SO_KEEPALIVE, true);

      // Why not using port 0 here which lets kernel pick an available port?
      // - Since another tony executor can bind to the same port when port 0 and SO_REUSEPORT are
      //   used together. See how a port is selected by kernel based on a free-list and socket
      //   options: https://idea.popcount.org/2014-04-03-bind-before-connect/#port-allocation.

      // Note it's still slightly possible that another tony processes grab the same port after
      // {@link #getAvailablePort()}, leading to two tensorflow process using the same port.
      // So adding an extra port check to reduce the risk.
      if (isPortAvailable(port)) {
        future = b.bind(port).await();
        if (!future.isSuccess()) {
          throw new BindException("Fail to bind to the port " + port);
        }
        return new ReusablePort(bossGroup, future);
      } else {
        LOG.info("Port " + port + " is no longer available.");
        throw new BindException("Fail to bind to the port" + port);
      }
    } catch (Exception e) {
      LOG.info("Reusable port allocation failed.", e);
      close(bossGroup, future);
      throw e;
    }
  }
}

