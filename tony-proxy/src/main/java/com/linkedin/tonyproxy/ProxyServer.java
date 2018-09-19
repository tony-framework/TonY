/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tonyproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This class is used to proxy requests from gateway to the cluster hosts.
 * Initial purpose of this class to to proxy requests to gateway to the hosts.
 */
public class ProxyServer {

  private static final Log LOG = LogFactory.getLog(ProxyServer.class);
  private String host;
  private int remotePort;
  private int localPort;
  public ProxyServer(String host, int remotePort, int localPort) {
    this.host = host;
    this.remotePort = remotePort;
    this.localPort = localPort;
  }
  public void start() throws IOException {
    LOG.info("Starting proxy for " + host + ":" + remotePort
             + " on port " + localPort);
    ServerSocket server = new ServerSocket(localPort);
    while (true) {
      new Proxy(server.accept(), host, remotePort).start();
    }
  }

  static class Proxy extends Thread {
    private Socket clientSocket;
    private final String serverHost;
    private final int serverPort;
    Proxy(Socket client, String host, int port) {
      this.clientSocket = client;
      this.serverHost = host;
      this.serverPort = port;
    }

    @Override
    public void run() {
      try {
        final byte[] request = new byte[1024];
        byte[] reply = new byte[4096];
        final InputStream inFromClient = clientSocket.getInputStream();
        final OutputStream outToClient = clientSocket.getOutputStream();
        Socket server;
        try {
          server = new Socket(serverHost, serverPort);
        } catch (IOException e) {
          PrintWriter out = new PrintWriter(new OutputStreamWriter(
              outToClient));
          out.flush();
          throw new RuntimeException(e);
        }
        final InputStream inFromServer = server.getInputStream();
        final OutputStream outToServer = server.getOutputStream();
        new Thread(() -> {
          int bytes_read;
          try {
            while ((bytes_read = inFromClient.read(request)) != -1) {
              outToServer.write(request, 0, bytes_read);
              outToServer.flush();
            }
          } catch (IOException e) {
            LOG.error(e);
          }
          try {
            outToServer.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }).start();
        int bytes_read;
        try {
          while ((bytes_read = inFromServer.read(reply)) != -1) {
            outToClient.write(reply, 0, bytes_read);
            outToClient.flush();
          }
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          try {
            server.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        outToClient.close();
        clientSocket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }
}
