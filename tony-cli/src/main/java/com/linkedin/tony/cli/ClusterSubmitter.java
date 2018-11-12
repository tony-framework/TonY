/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.cli;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.linkedin.tony.TonyClient;
import com.linkedin.tony.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class ClusterSubmitter {
  private static final Log LOG = LogFactory.getLog(ClusterSubmitter.class);

  private ClusterSubmitter() { }

  public static void main(String[] args) throws URISyntaxException {
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
      LOG.info("Copying " + jarLocation + " to: " + cachedLibPath);
      fs.mkdirs(cachedLibPath);
      fs.copyFromLocalFile(new Path(jarLocation), cachedLibPath);

      // Insert --hdfs_classpath at the beginning to avoid confusion when user pass in wrong arguments.
      List<String> updatedArgs = new LinkedList<>(Arrays.asList(args));
      updatedArgs.add(0, cachedLibPath.toString());
      updatedArgs.add(0, "--hdfs_classpath");
      exitCode = TonyClient.start(updatedArgs.toArray(new String[0]));
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      exitCode = -1;
    } finally {
      Utils.cleanupHDFSPath(hdfsConf, cachedLibPath);
    }
    System.exit(exitCode);
  }
}
