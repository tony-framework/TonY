/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.minitony.cluster.HDFSUtils;
import com.linkedin.minitony.cluster.MiniCluster;
import com.linkedin.minitony.cluster.MiniTonyUtils;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Before running these tests in your IDE, you should run {@code ligradle :tony:setupHdfsLib} first. If you make any
 * changes to {@code tony-core/src/main/java} code, you'll need to run the above command again.
 */
public class TestTonyE2E {

  private MiniCluster cluster;
  private String yarnConf;
  private String hdfsConf;

  @BeforeClass
  public void setup() throws Exception {
    // Set up mini cluster.
    cluster = new MiniCluster(3);
    cluster.start();
    yarnConf = Files.createTempFile("yarn", ".xml").toString();
    hdfsConf = Files.createTempFile("hdfs", ".xml").toString();
    Configuration yconf = cluster.getYarnConf();
    yconf.setBoolean("yarn.nodemanager.pmem-check-enabled", true);

    MiniTonyUtils.saveConfigToFile(yconf, yarnConf);
    MiniTonyUtils.saveConfigToFile(cluster.getHdfsConf(), hdfsConf);
    FileSystem fs = FileSystem.get(cluster.getHdfsConf());
    // This is the path we gonna store required libraries in the local HDFS.
    Path cachedLibPath = new Path("/yarn/libs");
    if (fs.exists(cachedLibPath)) {
      fs.delete(cachedLibPath, true);
    }
    fs.mkdirs(cachedLibPath);
    HDFSUtils.copyDirectoryFilesToFolder(fs, "tony-core/out/libs", "/yarn/libs");
    HDFSUtils.copyDirectoryFilesToFolder(fs, "tony-core/src/test/resources/log4j.properties", "/yarn/libs");
  }

  @AfterClass
  public void tearDown() {
    cluster.stop();
  }

  @Test
  public void testSingleNodeTrainingShouldPass() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, true);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    int exitCode = TonyClient.start(new String[] {
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldFailMissedHeartbeat() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    conf.setInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS, 2);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--container_env", Constants.TEST_TASK_EXECUTOR_NUM_HB_MISS + "=5"
    }, conf);
    Assert.assertNotEquals(exitCode, 0);
  }

  @Test
  public void testPSSkewedWorkerTrainingShouldPass() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    conf.setInt(TonyConfigurationKeys.getInstancesKey("worker"), 2);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--container_env", Constants.TEST_TASK_EXECUTOR_SKEW+ "=worker#0#30000"
    }, conf);
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldPass() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldFail() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_1.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testSingleNodeTrainingShouldFail() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, true);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_1.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testAMCrashTonyShouldFail() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, true);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.TEST_AM_CRASH + "=true",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, -1);
  }

  /**
   * The ps and workers should hang on the first attempt, and after 20 seconds, the AM should release the containers and
   * reschedule them in new containers.
   */
  @Test(enabled = false)
  public void testAMRequestsNewContainersAfterTimeout() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    conf.setInt(TonyConfigurationKeys.getInstancesKey("ps"), 2);
    conf.setInt(TonyConfigurationKeys.getInstancesKey("worker"), 2);
    conf.setInt(TonyConfigurationKeys.TASK_REGISTRATION_TIMEOUT_SEC, 20);
    conf.setInt(TonyConfigurationKeys.TASK_REGISTRATION_RETRY_COUNT, 1);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.TEST_TASK_EXECUTOR_HANG + "=true",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, 0);
  }

  /**
   * Tests that the AM will stop the job after a timeout period if the tasks have not registered yet (which suggests
   * they are stuck).
   */
  @Test
  public void testAMStopsJobAfterTimeout() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    conf.setInt(TonyConfigurationKeys.TASK_REGISTRATION_TIMEOUT_SEC, 5);
    conf.setInt(TonyConfigurationKeys.TASK_REGISTRATION_RETRY_COUNT, 0);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.TEST_TASK_EXECUTOR_HANG + "=true",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    }, conf);
    Assert.assertEquals(exitCode, -1);
  }

  /**
   * Test that makes sure if a worker is killed due to OOM, AM should stop the training (or retry).
   * This test might hang if there is a regression in handling the OOM scenario.
   */
  @Test
  public void testAMStopsJobAfterWorkerKilledByOOM() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    int exitCode = TonyClient.start(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--conf", "tony.worker.memory=1g",
        "--container_env", Constants.TEST_WORKER_TERMINATED + "=true"
    }, conf);
    Assert.assertEquals(exitCode, -1);
  }
}
