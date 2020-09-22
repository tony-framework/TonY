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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
  private final Process socketProcess;
  private final int port;
  public static final Path RESERVE_PORT_SCRIPT_PATH = requireNonNull(createPortReserveScript());
  public static final String PORT_FILE_NAME_SUFFIX = "___PORT___";

  private static Path createPortReserveScript() {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    final String reservePortScript = "reserve_reusable_port.py";
    try {
      // copy reserve_reusable_port.py from resource dir to a tmp dir
      Path tempDir = Files.createTempDirectory("reserve_reusable_port");
      tempDir.toFile().deleteOnExit();
      try (InputStream stream = classloader.getResourceAsStream(reservePortScript)) {
        Files.copy(stream, Paths.get(tempDir.toAbsolutePath().toString(), reservePortScript));
      }
      return Paths.get(tempDir.toAbsolutePath().toString(), reservePortScript);
    } catch (IOException ex) {
      return null;
    }
  }


  ReusablePort(Process socketProcess, int port) {
    this.socketProcess = socketProcess;
    this.port = port;
  }

  private void killSocketBindingProcess() {
    LOG.info("Killing the socket binding process..");
    this.socketProcess.destroy();
    int checkCount = 0, maxCheckCount = 10;
    while (this.socketProcess.isAlive() && (checkCount++) < maxCheckCount) {
      try {
        Thread.sleep(Duration.ofSeconds(1).toMinutes());
      } catch (InterruptedException e) {
        LOG.info(e);
      }
    }

    if (this.socketProcess.isAlive()) {
      LOG.info("Killing the socket binding process forcibly...");
      this.socketProcess.destroyForcibly();
    }

    LOG.info("Successfully killed the socket binding process");
  }

  /**
   * Closes the port.
   */
  @Override
  public void close() {
    killSocketBindingProcess();
  }

  /**
   * @return the binding port associated with the connection
   */
  @Override
  int getPort() {
    return this.port;
  }

  static boolean isPortAvailable(int port) {
    try (ServerSocket serverSocket = new ServerSocket()) {
      // setReuseAddress(false) is required only on OSX,
      // otherwise the code will not work correctly on that platform
      serverSocket.setReuseAddress(false);
      serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
      return true;
    } catch (Exception ex) {
      return false;
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
        LOG.info("Port binding attempt " + (i + 1) + " ....");
        reusablePort = create(getAvailablePort());
        return reusablePort;
      } catch (BindException ex) {
        LOG.info("Port binding attempt " + (i + 1) + " failed.");
      }
    }

    throw new BindException("Unable to bind port after " + portBindingRetry + " attempt(s).");
  }

  private static boolean waitTillPortReserved(int port) {
    Path fileToWait = Paths.get(RESERVE_PORT_SCRIPT_PATH.getParent().toString(), port + PORT_FILE_NAME_SUFFIX);
    int checkCount = 0, maxCheckCount = 5;
    while(!Files.exists(fileToWait) && (checkCount++) < maxCheckCount) {
      try {
        Duration checkInterval = Duration.ofSeconds(2);
        LOG.info(fileToWait + " doesn't exist, sleep for " + checkInterval.getSeconds() + " seconds");
        Thread.sleep(checkInterval.toMillis());
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
    return Files.exists(fileToWait);
  }

  /**
   * Creates a binding port with python which has built-in port reuse support.
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

    String socketBindingProcess = String.format("python %s -p %s -d %s",
        RESERVE_PORT_SCRIPT_PATH, port, Duration.ofHours(1).getSeconds());

    ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", socketBindingProcess);
    taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    if (isPortAvailable(port)) {
      LOG.info("Starting process " + socketBindingProcess);
      Process taskProcess = taskProcessBuilder.start();
      boolean portSuccessfulyCreated = waitTillPortReserved(port);
      if(!portSuccessfulyCreated) {
        LOG.info("Port " + port + " failed to be reserved.");
        taskProcess.destroy();
        throw new IOException("Fail to bind to the port " + port);
      }
      LOG.info("Port " + port + " is reserved.");
      return new ReusablePort(taskProcess, port);
    } else {
      LOG.info("Port " + port + " is no longer available.");
      throw new IOException("Fail to bind to the port " + port);
    }
  }
}

