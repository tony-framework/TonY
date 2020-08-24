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

import java.io.IOException;
import java.net.ServerSocket;

/**
 * This class represents an established socket connection. It's being used by TaskExecutor when
 * SO_REUSEPORT is not required. The reason for a separate {@link ServerSocket} implementation
 * for non-port-reuse cases is given {@link ReusablePort} with Netty's EpollEventLoopGroup only
 * works with Linux(https://netty.io/4.0/api/io/netty/channel/epoll/EpollEventLoopGroup.html),
 * having a separate implementation when SO_REUSEPORT is not being used enables tony, its e2e
 * unit tests and build on Mac.
 */
final class EphemeralPort extends ServerPort {

  final ServerSocket serverSocket;

  private EphemeralPort(ServerSocket serverSocket) {
    this.serverSocket = serverSocket;
  }

  /**
   * Creates a binding port which cannot be reused.
   * @return the binding port
   * @throws IOException when port allocation failed.
   */
  static EphemeralPort create() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    return new EphemeralPort(serverSocket);
  }

  @Override
  public void close() throws IOException {
    this.serverSocket.close();
  }

  @Override
  int getPort() {
    return this.serverSocket.getLocalPort();
  }
}
