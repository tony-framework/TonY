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

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.TonySession;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.util.Utils;
import com.sun.net.httpserver.HttpServer;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.ObjectWrapper;
import freemarker.template.Template;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class DashboardHttpServer implements AutoCloseable {
    private static final Log LOG = LogFactory.getLog(DashboardHttpServer.class);

    private static final String TEMPLATE_HTML_FILE = "template.html";

    private TonySession tonySession;
    private String amHostName;
    private String amLogUrl;
    private String runtimeType;
    private String appId;

    private String tensorboardUrl;

    private int serverPort;

    private ExecutorService executorService;

    private boolean started = false;

    private Template template;

    private DashboardHttpServer(
            @Nonnull TonySession tonySession, @Nonnull String amLogUrl,
            @Nonnull String runtimeType, @Nonnull String amHostName,
            @Nonnull String appId) {
        assert tonySession != null;
        this.tonySession = tonySession;
        this.amLogUrl = amLogUrl;
        this.runtimeType = runtimeType;
        this.amHostName = amHostName;
        this.appId = appId;
    }

    public String start() throws Exception {
        final int port = getAvailablePort();
        this.serverPort = port;
        LOG.info("Starting dashboard server, http url: " + amHostName + ":" + port);

        Configuration configuration = new Configuration();

        Path tempDir = Files.createTempDirectory("template");
        tempDir.toFile().deleteOnExit();
        Path templateFilePath = Paths.get(tempDir.toAbsolutePath().toString(), "template.html");
        try (InputStream stream = this.getClass().getClassLoader()
                .getResourceAsStream("dashboard/template.html")) {
            Files.copy(stream, templateFilePath);
        }
        configuration.setDirectoryForTemplateLoading(
                tempDir.toFile()
        );
        configuration.setObjectWrapper(new DefaultObjectWrapper());
        this.template = configuration.getTemplate(TEMPLATE_HTML_FILE);

        byte[] cssResource = IOUtils.toByteArray(
                this.getClass().getClassLoader()
                        .getResourceAsStream("dashboard/static/css/bootstrap.min.css")
        );
        byte[] logoResource = IOUtils.toByteArray(
                this.getClass().getClassLoader()
                        .getResourceAsStream("dashboard/static/img/TonY-icon-color.png")
        );

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

            server.createContext("/static/img/TonY-icon-color.png", httpExchange -> {
                httpExchange.getResponseHeaders().add("Content-Type", "image/png");
                httpExchange.sendResponseHeaders(200, logoResource.length);
                OutputStream out = httpExchange.getResponseBody();
                out.write(logoResource);
                out.close();
            });

            server.createContext("/static/css/bootstrap.min.css", httpExchange -> {
                httpExchange.getResponseHeaders().add("Content-Type", "text/css");
                httpExchange.sendResponseHeaders(200, cssResource.length);
                OutputStream out = httpExchange.getResponseBody();
                out.write(cssResource);
                out.close();
            });

            server.setExecutor(executorService);
            server.start();
            this.started = true;
        } catch (Throwable tr) {
            LOG.error("Errors on starting web dashboard server.", tr);
        }

        return amHostName + ":" + serverPort;
    }

    @VisibleForTesting
    protected int getServerPort() {
        return serverPort;
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
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("appMasterLogUrl", amLogUrl);
            dataMap.put("tensorboardLogUrl", tensorboardUrl == null ? "Not started yet." : tensorboardUrl);
            dataMap.put("runtimeType", runtimeType);
            dataMap.put("appId", StringUtils.defaultString(appId, ""));
            dataMap.put("amHostPort", String.format("http://%s:%s", amHostName, serverPort));

            int total = 0;
            int registered = 0;
            if (tonySession != null) {
                total = tonySession.getTotalTasks();
                registered = tonySession.getNumRegisteredTasks();
            }
            dataMap.put("registeredNumber", registered);
            dataMap.put("taskNumber", total);

            StringBuilder tableContentBuilder = new StringBuilder();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            if (tonySession != null) {
                tonySession.getTonyTasks().values().stream()
                        .flatMap(x -> Arrays.stream(x))
                        .filter(task -> task != null)
                        .filter(task -> task.getTaskInfo() != null)
                        .forEach(task -> {
                            TaskInfo taskInfo = task.getTaskInfo();

                            tableContentBuilder.append(String.format(
                                    "<tr> "
                                            + "<th scope=\"row\">%s</th> "
                                            + "<td>%s</td> "
                                            + "<td>%s</td> "
                                            + "<td>%s</td> "
                                            + "<td><a href=\"%s}\">LINK</td> "
                                            + "</tr>",
                                    taskInfo.getName() + ":" + taskInfo.getIndex(),
                                    taskInfo.getStatus().name(),
                                    task.getStartTime() == 0 ? "" : format.format(task.getStartTime()),
                                    task.getEndTime() == 0 ? "" : format.format(task.getEndTime()),
                                    taskInfo.getUrl()
                            ));
                        });
            }

            dataMap.put("tableContent", tableContentBuilder.toString());

            StringWriter writer = new StringWriter();
            template.process(dataMap, writer, ObjectWrapper.BEANS_WRAPPER);
            return writer.toString();
        } catch (Exception e) {
            LOG.error("Errors on returning html content.", e);
        }
        return "error";
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
        private String appId;

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

        public DashboardHttpServerBuilder appId(String appId) {
            this.appId = appId;
            return this;
        }

        public DashboardHttpServer build() {
            return new DashboardHttpServer(tonySession, amLogUrl, runtimeType, amHostName, appId);
        }
    }
}
