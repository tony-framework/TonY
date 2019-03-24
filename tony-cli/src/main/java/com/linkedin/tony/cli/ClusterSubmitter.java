/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.cli;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.util.Utils;
import java.util.UUID;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.exceptions.YarnException;

import static com.linkedin.tony.Constants.*;


/**
 * ClusterSubmitter is used to submit a distributed Tony
 * job on the cluster.
 *
 * Usage:
 * java -cp tony-cli-x.x.x-all.jar com.linkedin.tony.cli.ClusterSubmitter
 * --src_dir /Users/xxx/hadoop/li-tony_trunk/tony-core/src/test/resources/ \
 * --executes /Users/xxx/hadoop/li-tony_trunk/tony/src/test/resources/exit_0_check_env.py \
 * --python_binary_path python
 */
public class ClusterSubmitter extends TonySubmitter {
  private static final Log LOG = LogFactory.getLog(ClusterSubmitter.class);
  private TonyClient client;

  private ClusterSubmitter() {
    this(new TonyClient(new Configuration()));
  }
  public ClusterSubmitter(TonyClient client) {
    this.client = client;
  }

  public int submit(String[] args) throws ParseException, URISyntaxException {
    LOG.info("Starting ClusterSubmitter..");
    String jarLocation = new File(ClusterSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    Configuration hdfsConf = new Configuration();
    hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
    hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    LOG.info(hdfsConf);
    int exitCode;
    Path cachedLibPath = null;
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      cachedLibPath = new Path(fs.getHomeDirectory(), TONY_FOLDER + Path.SEPARATOR + UUID.randomUUID().toString());
      Utils.uploadFileAndSetConfResources(cachedLibPath, new Path(jarLocation), TONY_JAR_NAME, client.getTonyConf(), fs,
          LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
      LOG.info("Copying " + jarLocation + " to: " + cachedLibPath);
      boolean sanityCheck = client.init(args);
      if (!sanityCheck) {
        LOG.error("Arguments failed to pass sanity check");
        return -1;
      }
      // ensure application is killed when this process terminates
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          client.forceKillApplication();
        } catch (YarnException | IOException e) {
          LOG.error("Failed to kill application during shutdown.", e);
        }
      }));
      exitCode = client.start();
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      exitCode = -1;
    } finally {
      Utils.cleanupHDFSPath(hdfsConf, cachedLibPath);
    }
    return exitCode;
  }

  public static void main(String[] args) throws ParseException, URISyntaxException {
    int exitCode;
    ClusterSubmitter submitter = new ClusterSubmitter();
    exitCode = submitter.submit(args);
    System.exit(exitCode);
  }
}
