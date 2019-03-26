/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.client.TaskUpdateListener;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tonyproxy.ProxyServer;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;

import static com.linkedin.tony.Constants.*;

/**
 * NotebookSubmitter is used to submit a python pex file (for example Jupyter Notebook) to run inside a cluster.
 *
 * It would first kick off a container inside the cluster that matches the resource request
 * (am GPU/Memory/CPU) and run the specified script inside that node. To make it easier for
 * Jupyter Notebook, we also bake in a proxy server in the submitter which would automatically
 * proxy the request to that node.
 *
 * Usage:
 * // Suppose you have a folder named bin/ at root directory which contains a notebook pex file: linotebook, you can use
 * // this command to start the notebook and follow the output message to visit the jupyter notebook page.
 * CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob):./:/home/khu/notebook/tony-cli-0.1.0-all.jar \
 * java com.linkedin.tony.cli.NotebookSubmitter --src_dir bin/ --executes "'bin/linotebook --ip=* $DISABLE_TOKEN'"
 */
public class NotebookSubmitter extends TonySubmitter {
  private static final Log LOG = LogFactory.getLog(NotebookSubmitter.class);

  private static class NotebookUpdateListener implements TaskUpdateListener {
    private Set<TaskInfo> taskInfos;

    Set<TaskInfo> getTaskInfos() {
      return taskInfos;
    }

    @Override
    public void onTaskInfosUpdated(Set<TaskInfo> taskInfoSet) {
      this.taskInfos = taskInfoSet;
    }
  }

  private NotebookUpdateListener listener;
  private TonyClient client;

  public NotebookSubmitter() {
    listener = new NotebookUpdateListener();
    client = new TonyClient(new Configuration());
    client.addListener(listener);
  }

  public int submit(String[] args)
      throws ParseException, URISyntaxException, IOException, InterruptedException {
    LOG.info("Starting NotebookSubmitter..");
    String jarPath = new File(NotebookSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();

    int exitCode = 0;
    Path cachedLibPath;
    Configuration hdfsConf = new Configuration();
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      cachedLibPath = new Path(fs.getHomeDirectory(), TONY_FOLDER + Path.SEPARATOR + UUID.randomUUID().toString());
      fs.mkdirs(cachedLibPath);
      fs.copyFromLocalFile(new Path(jarPath), cachedLibPath);
      LOG.info("Copying " + jarPath + " to: " + cachedLibPath);
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      return -1;
    }

    String[] updatedArgs = Arrays.copyOf(args, args.length + 4);
    updatedArgs[args.length] = "--hdfs_classpath";
    updatedArgs[args.length + 1] = cachedLibPath.toString();
    updatedArgs[args.length + 2] = "--conf";
    updatedArgs[args.length + 3] = TonyConfigurationKeys.APPLICATION_TIMEOUT + "=" + (24 * 60 * 60 * 1000);

    client.init(updatedArgs);
    Thread clientThread = new Thread(client::start);

    // ensure notebook application is killed when this client process terminates
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        client.forceKillApplication();
      } catch (YarnException | IOException e) {
        LOG.error("Failed to kill application during shutdown.", e);
      }
    }));
    clientThread.start();
    while (clientThread.isAlive()) {
      Set<TaskInfo> taskInfos = listener.getTaskInfos();
      if (taskInfos != null) {
        for (TaskInfo taskInfo : taskInfos) {
          if (taskInfo.getName().equals(Constants.NOTEBOOK_JOB_NAME)) {
            URL url = new URL(taskInfo.getUrl());
            String host = url.getHost();
            int port = url.getPort();
            ServerSocket localSocket = new ServerSocket(0);
            int localPort = localSocket.getLocalPort();
            localSocket.close();
            ProxyServer server = new ProxyServer(host, port, localPort);
            LOG.info("If you are running NotebookSubmitter in your local box, please open [localhost:" + localPort
                + "] in your browser to visit the page. Otherwise, if you're running NotebookSubmitter in a remote "
                + "machine (like a gateway), please run" + " [ssh -L 18888:localhost:" + localPort
                + " name_of_this_host] in your laptop and open [localhost:18888] in your browser to visit Jupyter "
                + "Notebook. If the 18888 port is occupied, replace that number with another number.");
            server.start();
            break;
          }
        }
      }
      Thread.sleep(1000);
    }
    clientThread.join();
    return exitCode;
  }

  public TonyClient getClient() {
    return client;
  }

  public static void main(String[] args) throws  Exception {
    int exitCode;
    NotebookSubmitter submitter = new NotebookSubmitter();
    exitCode = submitter.submit(args);
    System.exit(exitCode);
  }

}
