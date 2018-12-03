/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.minitony.cluster.HDFSUtils;
import com.linkedin.minitony.cluster.MiniCluster;
import com.linkedin.minitony.cluster.MiniTonyUtils;
import java.nio.file.Files;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Before running these tests in your IDE, you should run {@code ./gradlew
 * :tony-core:setupHdfsLib} first. If you make any changes to {@code
 * tony-core/src/main/java} code, you'll need to run the above command again.
 */
public class TestTonyE2E {

  private MiniCluster cluster;
  private String yarnConf;
  private String hdfsConf;
  private Configuration conf = new Configuration();
  private TonyClient client;

  @BeforeClass
  public void doBeforeClass() throws Exception {
    // Set up mini cluster.
    cluster = new MiniCluster(3);
    cluster.start();
    yarnConf = Files.createTempFile("yarn", ".xml").toString();
    hdfsConf = Files.createTempFile("hdfs", ".xml").toString();
    MiniTonyUtils.saveConfigToFile(cluster.getYarnConf(), yarnConf);
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
  public void doAfterClass() {
    cluster.stop();
  }

  @BeforeMethod
  public void doBeforeMethod() {
    conf = new Configuration();
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    client = new TonyClient(conf);
  }

  @Test
  public void testSingleNodeTrainingShouldPass() throws ParseException {
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, true);
    client = new TonyClient(conf);
    client.init(new String[] {
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldFailMissedHeartbeat() throws ParseException {
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.setInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS, 2);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--container_env", Constants.TEST_TASK_EXECUTOR_NUM_HB_MISS + "=5"
    });
    int exitCode = client.start();
    Assert.assertNotEquals(exitCode, 0);
  }

  @Test
  public void testPSSkewedWorkerTrainingShouldPass() throws ParseException {
    conf.setInt(TonyConfigurationKeys.getInstancesKey("worker"), 2);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--container_env", Constants.TEST_TASK_EXECUTOR_SKEW+ "=worker#0#30000"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldPass() throws ParseException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_env.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingPyTorchShouldPass() throws ParseException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0_check_pytorchenv.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--conf", "tony.application.framework=pytorch",
        "--conf", "tony.ps.instances=0",
        "--conf", "tony.worker.instances=2",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldFail() throws ParseException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_1.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testSingleNodeTrainingShouldFail() throws ParseException {
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, true);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_1.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testAMCrashTonyShouldFail() throws ParseException {
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, true);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.TEST_AM_CRASH + "=true",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  /**
   * Test that makes sure if a worker is killed due to OOM, AM should stop the training (or retry).
   * This test might hang if there is a regression in handling the OOM scenario.
   *
   * The reason why we use a Constants.TEST_WORKER_TERMINATED flag instead of simply requesting more memory than
   * allocated is that Physical Memory Enforcement doesn't seem to work under MiniYARN.
   */
  @Test
  public void testAMStopsJobAfterWorker0Killed() throws ParseException {
    client.init(new String[]{"--src_dir", "tony-core/src/test/resources/", "--executes", "tony-core/src/test/resources/exit_0.py", "--hdfs_classpath", "/yarn/libs", "--python_binary_path", "python", "--container_env",
        Constants.TEST_WORKER_TERMINATED + "=true"});
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  /**
   * Test amRpcClient is nulled out after client finishes.
   */
  @Test
  public void testNullAMRpcClient() throws Exception {
    String[] args = new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_0.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python"
    };
    client.init(args);
    Assert.assertTrue(client.start() == 0);
    Assert.assertNull(client.getAMRpcClient());
  }

  @Test
  public void testNonChiefWorkerFail() throws ParseException {
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, false);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/",
        "--executes", "tony-core/src/test/resources/exit_1.py",
        "--hdfs_classpath", "/yarn/libs",
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testTonyResourcesFlag() throws ParseException {
    conf.setBoolean(TonyConfigurationKeys.IS_SINGLE_NODE, false);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--executes", "'/bin/cat log4j.properties'",
        "--hdfs_classpath", "/yarn/libs",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--conf", "tony.worker.resources=/yarn/libs",
        "--conf", "tony.ps.instances=0",
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }
}
