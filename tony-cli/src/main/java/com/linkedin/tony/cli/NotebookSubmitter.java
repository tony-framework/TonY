/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

import com.linkedin.tony.TonyClient;
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
    Options opts = new Options();
    opts.addOption("file_url", true, "The file to be downloaded.");
    opts.addOption("exec", true, "The file to be executed inside the downloaded archive file.");
    opts.addOption("timeout", true, "the timeout to stop notebook executor, in seconds.");

    String jarPath = new File(NotebookSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    CommandLine cliParser = new GnuParser().parse(opts, args);
    String fileUrl = cliParser.getOptionValue("file_url", "");
    String exec = cliParser.getOptionValue("exec", "");
    int timeout = Integer.parseInt(cliParser.getOptionValue("timeout", "3600000"));
    timeout = Math.min(timeout, 24 * 3600 * 1000);
    InputStream in = new URL(fileUrl).openStream();
    String fileName = "notebook.zip";
    Files.copy(in, Paths.get(fileName), StandardCopyOption.REPLACE_EXISTING);

    int exitCode = 0;
    Path cachedLibPath = null;
    Configuration hdfsConf = new Configuration();
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      cachedLibPath = new Path(fs.getHomeDirectory(), TONY_FOLDER + Path.SEPARATOR + UUID.randomUUID().toString());
      fs.mkdirs(cachedLibPath);
      fs.copyFromLocalFile(new Path(jarPath), cachedLibPath);
      LOG.info("Copying " + jarPath + " to: " + cachedLibPath);
      fs.copyFromLocalFile(new Path(fileName), cachedLibPath);
      LOG.info("Copying " + fileName + " to: " + cachedLibPath);
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      exitCode = -1;
    }
    if (cachedLibPath == null) {
      System.exit(-1);
    }
    String[] tonyArgumentsArray = new String[]{
        "--src_dir", fileName,
        "--executes", exec,
        "--hdfs_classpath", cachedLibPath.toString(),
    };

    TonyClient client = TonyClient.createClientInstance(tonyArgumentsArray, new Configuration());
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
        server.start();
        break;
      }
    }
    Thread.sleep(timeout);
    System.exit(exitCode);

  }
}