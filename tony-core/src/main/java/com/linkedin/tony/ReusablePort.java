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
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
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
  final Process socketProcess;
  final int port;

  ReusablePort(Process socketProcess, int port) {
    this.socketProcess = socketProcess;
    this.port = port;
  }

  /**
   * Closes the port.
   */
  @Override
  public void close() {
    this.socketProcess.destroy();
  }

  /**
   * @return the binding port associated with the connection
   */
  @Override
  int getPort() {
    return this.port;
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
  static ReusablePort create() throws IOException {
    ReusablePort reusablePort;
    final int portBindingRetry = 5;
    for (int i = 0; i < portBindingRetry; i++) {
      try {
        LOG.info("port binding attempt " + (i + 1) + " ....");
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
  static ReusablePort create(int port) throws IOException {
    // Why not upgrading Tony to Java 9+ given port reuse is supported in Java 9+?
    // - In Linkedin, as of now(2020/08), only Java 8 and 11 are officially supported, but Java 11
    //   introduces incompatibility with Play version tony-portal
    //   (https://github.com/linkedin/TonY/tree/master/tony-portal) is using. Upgrading Play to a
    //   Java 11-compatible version requires non-trivial amount of effort.

    Preconditions.checkArgument(port > 0, "Port must > 0.");
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    // copy portreuse
    Path tempDir = Files.createTempDirectory("reserve_reusable_port");
    tempDir.toFile().deleteOnExit();
    final String reservePortScript = "reserve_reusable_port.py";
    try (InputStream stream = classloader.getResourceAsStream(reservePortScript)) {
      Files.copy(stream, Paths.get(tempDir.toAbsolutePath().toString(), reservePortScript));
    }


    String bindSocket = String.format("/export/apps/python/3.7/bin/python3 %s -p %s -t %s",
        Paths.get(tempDir.toAbsolutePath().toString(), reservePortScript),
        port,
        Duration.ofHours(1).getSeconds());

    ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", bindSocket);
    taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    if (isPortAvailable(port)) {
      LOG.info("starting process " + bindSocket);
      Process taskProcess = taskProcessBuilder.start();
      return new ReusablePort(taskProcess, port);
    } else {
      LOG.info("Port " + port + " is no longer available.");
      throw new BindException("Fail to bind to the port " + port);
    }
  }
}

