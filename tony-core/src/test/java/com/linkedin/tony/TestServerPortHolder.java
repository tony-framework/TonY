/**
 * Copyright 2021 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;

import org.testng.annotations.Test;

public class TestServerPortHolder {

    @Test(expectedExceptions = BindException.class)
    public void testPortHolderShouldFail() throws IOException {
        int freePort = ServerPortHolder.getFreePort();
        ServerPortHolder holder = new ServerPortHolder(freePort);
        holder.start();

        try (ServerSocket occupiedSocket = new ServerSocket(freePort)) {
            InetAddress inetAddress = InetAddress.getByName("localhost");
            SocketAddress endPoint = new InetSocketAddress(inetAddress, freePort);
            occupiedSocket.bind(endPoint);
        }
    }

    @Test
    public void testPortHolderCloseShouldPass() throws IOException {
        int freePort = ServerPortHolder.getFreePort();
        ServerPortHolder holder = new ServerPortHolder(freePort);
        holder.start();

        holder.close();

        try (ServerSocket occupiedSocket = new ServerSocket()) {
            InetAddress inetAddress = InetAddress.getByName("localhost");
            SocketAddress endPoint = new InetSocketAddress(inetAddress, freePort);
            occupiedSocket.bind(endPoint);
        }
    }
}
