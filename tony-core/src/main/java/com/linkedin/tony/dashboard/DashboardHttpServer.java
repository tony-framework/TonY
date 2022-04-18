/*
 * Copyright 2022 LinkedIn Corp.
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
package com.linkedin.tony.dashboard;

import com.linkedin.tony.TonySession;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.util.Utils;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DashboardHttpServer implements AutoCloseable {
    private static final Log LOG = LogFactory.getLog(DashboardHttpServer.class);

    private TonySession tonySession;
    private String amHostName;
    private String amLogUrl;
    private String runtimeType;

    private String tensorboardUrl;

    private int serverPort;

    private ExecutorService executorService;

    private boolean started = false;

    private DashboardHttpServer(
            @Nonnull TonySession tonySession, @Nonnull String amLogUrl,
            @Nonnull String runtimeType, @Nonnull String amHostName) {
        this.tonySession = tonySession;
        this.amLogUrl = amLogUrl;
        this.runtimeType = runtimeType;
        this.amHostName = amHostName;
    }

    public String start() throws Exception {
        final int port = getAvailablePort();
        this.serverPort = port;
        LOG.info("Starting dashboard server, http url: " + amHostName + ":" + port);

        this.executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

                server.createContext("/", httpExchange -> {
                    byte[] response = getDashboardContent().getBytes("UTF-8");

                    httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
                    httpExchange.sendResponseHeaders(200, response.length);

                    OutputStream out = httpExchange.getResponseBody();
                    out.write(response);
                    out.close();
                });
                server.start();
                this.started = true;
            } catch (Throwable tr) {
                LOG.error("Errors on starting web dashboard server.", tr);
            }
        });
        return amHostName + ":" + serverPort;
    }

    private int getAvailablePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    public void updateTonySession(TonySession session) {
        this.tonySession = session;
    }

    public void registerTensorboardUrl(String tbUrl) {
        this.tensorboardUrl = tbUrl;
    }

    private String getDashboardContent() {
        /**
         * TODO: Need to introduce the template engine to support pretty html page.
         */
        StringBuilder builder = new StringBuilder();
        builder.append("<!DOCTYPE html><html>");
        builder.append("<head>");
        builder.append("<title>TonY Dashboard</title>");
        builder.append("<style> table, th, td {  border: 1px solid black;  border-collapse: collapse; } th, td {"
                + "  padding: 8px; } </style>");
        builder.append("</head>");
        builder.append("<body>");

        builder.append("<h2>TonY Dashboard</h2>");
        builder.append("<hr/>");
        builder.append("<p>ApplicationMaster log url: <a href=\"" + amLogUrl + "\">" + amLogUrl + "</a></p>");
        if (tensorboardUrl != null) {
            builder.append("<p>Tensorboard log url: <a href=\"" + tensorboardUrl + "\">"
                    + tensorboardUrl + "</a></p>");
        } else {
            builder.append("<p>Tensorboard log url: (not started yet)</p>");
        }
        builder.append("<p>Runtime type: " + runtimeType + "</p>");

        if (tonySession != null) {
            int total = tonySession.getTotalTasks();
            int registered = tonySession.getNumRegisteredTasks();
            builder.append("<p>Total task executors: "
                    + total
                    + ",    registered task executors: "
                    + registered
                    + "</p>"
            );
        }

        builder.append("<hr/>");
        builder.append("<h4>Task Executors</h4>");

        if (tonySession != null) {
            builder.append("<table style=\"width:100%\">"
                    + "  <tr>"
                    + "  <th>task executor</th>"
                    + "    <th>state</th>"
                    + "    <th>log url</th>"
                    + "  </tr>");
            tonySession.getTonyTasks().values().stream()
                    .flatMap(x -> Arrays.stream(x))
                    .filter(task -> task != null)
                    .filter(task -> task.getTaskInfo() != null)
                    .forEach(task -> {
                        TaskInfo taskInfo = task.getTaskInfo();
                        builder.append("<tr>"
                                + "    <td>" + taskInfo.getName() + ":" + taskInfo.getIndex() + "</td>"
                                + "    <td>" + taskInfo.getStatus() + "</td>"
                                + "    <td><a href=\"" + taskInfo.getUrl() + "\">" + taskInfo.getUrl() + "</a></td>"
                                + "  </tr>");
                    });
            builder.append("</table>");
        }

        builder.append("</body>");
        builder.append("</html>");

        return builder.toString();
    }

    /**
     *  Just for test case.
     */
    protected boolean isStarted() {
        return started;
    }

    public static DashboardHttpServerBuilder builder() {
        return new DashboardHttpServerBuilder();
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            Utils.shutdownThreadPool(executorService);
            this.executorService = null;
        }
    }

    public static class DashboardHttpServerBuilder {
        private TonySession tonySession;
        private String amLogUrl;
        private String runtimeType;
        private String amHostName;

        private DashboardHttpServerBuilder() {
            // ignore
        }

        public DashboardHttpServerBuilder session(TonySession session) {
            this.tonySession = session;
            return this;
        }

        public DashboardHttpServerBuilder amLogUrl(String amLogUrl) {
            this.amLogUrl = amLogUrl;
            return this;
        }

        public DashboardHttpServerBuilder runtimeType(String runtimeType) {
            this.runtimeType = runtimeType;
            return this;
        }

        public DashboardHttpServerBuilder amHostName(String amHostName) {
            this.amHostName = amHostName;
            return this;
        }

        public DashboardHttpServer build() {
            return new DashboardHttpServer(tonySession, amLogUrl, runtimeType, amHostName);
        }
    }
}
