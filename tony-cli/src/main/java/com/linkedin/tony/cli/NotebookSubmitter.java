/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.Utils;
import com.linkedin.tonyproxy.ProxyServer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;

import static com.linkedin.tony.Constants.*;


public class NotebookSubmitter {
  private static final Log LOG = LogFactory.getLog(NotebookSubmitter.class);

  private NotebookSubmitter() { }

  public static void main(String[] args) throws  Exception {
    LOG.info("Starting NotebookSubmitter..");
    String jarPath = new File(NotebookSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    Options opts = Utils.getCommonOptions();
    opts.addOption("conf", true, "User specified configuration, as key=val pairs");
    opts.addOption("conf_file", true, "Name of user specified conf file, on the classpath");
    opts.addOption("src_dir", true, "Name of directory of source files.");

    int exitCode = 0;
    Path cachedLibPath = null;
    Configuration hdfsConf = new Configuration();
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      cachedLibPath = new Path(fs.getHomeDirectory(), TONY_FOLDER + Path.SEPARATOR + UUID.randomUUID().toString());
      fs.mkdirs(cachedLibPath);
      fs.copyFromLocalFile(new Path(jarPath), cachedLibPath);
      LOG.info("Copying " + jarPath + " to: " + cachedLibPath);
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      exitCode = -1;
    }
    if (cachedLibPath == null) {
      System.exit(-1);
    }

    String[] updatedArgs = Arrays.copyOf(args, args.length + 4);
    updatedArgs[args.length] = "--hdfs_classpath";
    updatedArgs[args.length + 1] = cachedLibPath.toString();
    updatedArgs[args.length + 2] = "--conf";
    updatedArgs[args.length + 3] = TonyConfigurationKeys.APPLICATION_TIMEOUT + "=" + String.valueOf(24 * 3600 *1000);

    TonyClient client = TonyClient.createClientInstance(updatedArgs, new Configuration());
    if (client == null) {
      System.exit(-1);
    }
    Thread clientThread = new Thread(() -> {
      try {
        client.run();
      } catch (IOException | URISyntaxException | YarnException | InterruptedException e) {
        LOG.error(e);
      }
    });
    clientThread.start();
    while (clientThread.isAlive()) {
      if (client.notebookUrl != null) {
        String[] hostPort = client.notebookUrl.split(":");
        ServerSocket localSocket = new ServerSocket(0);
        int localPort = localSocket.getLocalPort();
        localSocket.close();
        ProxyServer server = new ProxyServer(hostPort[0], Integer.parseInt(hostPort[1]), localPort);
        LOG.info("Please run [ssh -L 8080:localhost:" + String.valueOf(localPort) + " name_of_this_host] in your laptop and open [localhost:8080] in your browser to visit Notebook");
        server.start();
        break;
      }
    }
    clientThread.join();
    System.exit(exitCode);

  }
}
