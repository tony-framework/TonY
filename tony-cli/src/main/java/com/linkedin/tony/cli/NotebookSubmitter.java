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
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;


public class NotebookSubmitter {
  private static final Log LOG = LogFactory.getLog(NotebookSubmitter.class);

  private NotebookSubmitter() { }

  public static void main(String[] args) throws  Exception {
    LOG.info("Starting NotebookSubmitter..");
    Options opts = new Options();
    opts.addOption("file_url", true, "The file to be downloaded.");
    opts.addOption("exec", true, "The file to be executed inside the downloaded archive file.");
    opts.addOption("timeout", true, "the timeout to stop notebook executor, in seconds.");

    String pattern = "yyyy-MM-dd";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    String date = simpleDateFormat.format(new Date());
    String jarPath = new File(NotebookSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    CommandLine cliParser = new GnuParser().parse(opts, args);
    String fileUrl = cliParser.getOptionValue("file_url", "");
    String exec = cliParser.getOptionValue("exec", "");
    int timeout = Integer.parseInt(cliParser.getOptionValue("timeout", "3600000"));
    timeout = Math.min(timeout, 24 * 3600 * 1000);
    InputStream in = new URL(fileUrl).openStream();
    String fileName = "notebook.zip";
    Files.copy(in, Paths.get(fileName), StandardCopyOption.REPLACE_EXISTING);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    String tmpDirBase = conf.get("hadoop.tmp.dir");

    String tmpPath = tmpDirBase + File.separatorChar + UserGroupInformation.getCurrentUser().getShortUserName() + date;

    // This is the path we gonna store required libraries in the local HDFS.
    Path cachedLibPath = new Path(tmpPath);
    if (fs.exists(cachedLibPath)) {
      fs.delete(cachedLibPath, true);
    }
    fs.mkdirs(cachedLibPath);
    fs.copyFromLocalFile(new Path(jarPath), new Path(tmpPath));
    String tonyArguments = "";
    tonyArguments += " --hdfs_classpath " + tmpPath;
    tonyArguments += " --src " + fileName;
    tonyArguments += " --exec " + exec;
    tonyArguments += " --singleNode";
    String[] tonyArgumentsArray = tonyArguments.split(" ");

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

  }
}