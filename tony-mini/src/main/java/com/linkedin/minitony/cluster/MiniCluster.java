/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.minitony.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;


/**
 * MiniCluster is used to spin off a Mini Hadoop cluster. This can be used independently
 * inside TonY itself for integration testing.
 */
public class MiniCluster {
  private static final Log LOG = LogFactory.getLog(MiniCluster.class);
  private MiniDFSCluster dfsCluster;
  private MiniYARNCluster yarnCluster;

  private static final short REPLICATION = 1;
  private static final int BLOCKSIZE = 1048576;
  private Configuration yarnClusterConf;
  private Configuration hdfsClusterConf;
  private int numNodeManagers;

  /**
   * Instantiate a MiniCluster instance.
   * @param numNodeManagers the number of nodes inside mini cluster.
   */
  public MiniCluster(int numNodeManagers) {
    this.numNodeManagers = numNodeManagers;
  }

  public void start() throws Exception {
    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
    yarnConf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, false);
    yarnConf.setClass(YarnConfiguration.RM_SCHEDULER,
                      FifoScheduler.class, ResourceScheduler.class);
    HdfsConfiguration hdfsConf = new HdfsConfiguration();
    hdfsConf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    yarnCluster = new MiniYARNCluster("MiniTonY", numNodeManagers, 1, 1);
    dfsCluster = new MiniDFSCluster.Builder(hdfsConf).numDataNodes(1).numDataNodes(REPLICATION).build();
    yarnCluster.init(yarnConf);
    yarnCluster.start();
    dfsCluster.waitActive();
    yarnClusterConf = yarnCluster.getConfig();
    hdfsClusterConf = dfsCluster.getConfiguration(0);
    yarnClusterConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
    hdfsClusterConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
  }

  public void stop() {
    yarnCluster.stop();
    dfsCluster.shutdown();
  }

  public Configuration getYarnConf() {
    return yarnClusterConf;
  }

  public Configuration getHdfsConf() {
    return hdfsClusterConf;
  }

  public static void main(String[] args) {
    try {
      MiniCluster cluster = new MiniCluster(2);
      cluster.start();
      cluster.stop();
    } catch (Exception e) {
      LOG.error(e);
    }
  }
}
