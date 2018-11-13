/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.cli;

import com.linkedin.minitony.cluster.HDFSUtils;
import com.linkedin.minitony.cluster.MiniCluster;
import com.linkedin.minitony.cluster.MiniTonyUtils;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * LocalSubmitter is used to spin off a local Hadoop cluster and execute a distributed Tony
 * job on that cluster.
 *
 * Example usage:
 * java -cp tony-cli-x.x.x-all.jar com.linkedin.tony.cli.LocalSubmitter \
 * --src_dir /Users/xxx/hadoop/li-tony_trunk/tony-core/src/test/resources/ \
 * --executes /Users/xxx/hadoop/li-tony_trunk/tony/src/test/resources/exit_0_check_env.py \
 * --python_binary_path python \
 */
public class LocalSubmitter {
  private static final Log LOG = LogFactory.getLog(ClusterSubmitter.class);
  private static final int NUM_NODE_MANAGERS = 2;

  private LocalSubmitter() { }

  public static void main(String[] args) throws  Exception {
    LOG.info("Starting LocalSubmitter..");
    String jarLocation = new File(ClusterSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    MiniCluster cluster = new MiniCluster(NUM_NODE_MANAGERS);
    cluster.start();
    String yarnConf = Files.createTempFile("yarn", ".xml").toString();
    String hdfsConf = Files.createTempFile("hdfs", ".xml").toString();

    MiniTonyUtils.saveConfigToFile(cluster.getYarnConf(), yarnConf);
    MiniTonyUtils.saveConfigToFile(cluster.getHdfsConf(), hdfsConf);
    FileSystem fs = FileSystem.get(cluster.getHdfsConf());
    // This is the path we gonna store required libraries in the local HDFS.
    Path cachedLibPath = new Path("/yarn/libs");
    if (fs.exists(cachedLibPath)) {
      fs.delete(cachedLibPath, true);
    }
    fs.mkdirs(cachedLibPath);
    HDFSUtils.copyDirectoryFilesToFolder(fs, jarLocation, "/yarn/libs");
    int exitCode;
    Configuration conf = new Configuration();
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    // Append other required parameters for TonyClient
    String[] updatedArgs = Arrays.copyOf(args, args.length + 2);
    updatedArgs[args.length] = "--hdfs_classpath";
    updatedArgs[args.length + 1] = cachedLibPath.toString();
    TonyClient client = new TonyClient(conf);
    client.init(updatedArgs);
    exitCode = client.start();
    cluster.stop();
    System.exit(exitCode);
  }
}
