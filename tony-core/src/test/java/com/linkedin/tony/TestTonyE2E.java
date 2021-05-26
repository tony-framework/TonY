/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.minitony.cluster.HDFSUtils;
import com.linkedin.minitony.cluster.MiniCluster;
import com.linkedin.minitony.cluster.MiniTonyUtils;
import com.linkedin.tony.client.CallbackHandler;
import com.linkedin.tony.client.TaskUpdateListener;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.impl.TaskStatus;
import java.util.HashSet;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Set;

import static com.linkedin.tony.TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS;
import static com.linkedin.tony.TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;


/**
 * Before running these tests in your IDE, you should run {@code ./gradlew
 * :tony-core:setupHdfsLib} first. If you make any changes to {@code
 * tony-core/src/main/java} code, you'll need to run the above command again.
 *
 * If you get an exception saying there's "no such file or directory tony-core/out/libs",
 * you will need to update the working directory in your test configuration
 * to {@code /path/to/linkedin/TonY}.
 *
 * The YARN logs for the test should be in {@code <TonY>/target/MiniTonY}.
 */
public class TestTonyE2E  {

  private static class TestTonyE2EHandler implements CallbackHandler, TaskUpdateListener {

    private ApplicationId appId;

    public ApplicationId getAppId() {
      return appId;
    }

    public Set<TaskInfo> getTaskInfoSet() {
      return taskInfoSet;
    }

    private Set<TaskInfo> taskInfoSet;

    @Override
    public void onApplicationIdReceived(ApplicationId appId) {
      this.appId = appId;
    }

    @Override
    public void onTaskInfosUpdated(Set<TaskInfo> taskInfoSet) {
      this.taskInfoSet = taskInfoSet;
    }
  }

  private MiniCluster cluster;
  private String yarnConf;
  private String hdfsConf;
  private Configuration conf = new Configuration();
  private TonyClient client;
  private String libPath;
  private TestTonyE2EHandler handler;

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
    libPath = cluster.getHdfsConf().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY) + "/yarn/lib";
    // This is the path we gonna store required libraries in the local HDFS.
    Path cachedLibPath = new Path(libPath);
    if (fs.exists(cachedLibPath)) {
      fs.delete(cachedLibPath, true);
    }
    fs.mkdirs(cachedLibPath);
    HDFSUtils.copyDirectoryFilesToFolder(fs, "tony-core/out/libs", libPath);
    HDFSUtils.copyDirectoryFilesToFolder(fs, "tony-core/src/test/resources/log4j.properties", libPath);
  }

  @AfterClass
  public void doAfterClass() {
    cluster.stop();
  }

  @BeforeMethod
  public void doBeforeMethod() {
    handler = new TestTonyE2EHandler();
    conf = new Configuration();
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.set(TonyConfigurationKeys.HDFS_CONF_LOCATION, hdfsConf);
    conf.set(TonyConfigurationKeys.YARN_CONF_LOCATION, yarnConf);
    conf.set(TonyConfigurationKeys.getContainerResourcesKey(), "tony-core/src/test/resources/common.zip");
    client = new TonyClient(handler, conf);
  }

  @Test
  public void testSingleNodeTrainingShouldPass() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[] {
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0_check_env.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldFailMissedHeartbeat() throws ParseException, IOException {
    conf.setBoolean(TonyConfigurationKeys.SECURITY_ENABLED, false);
    conf.setInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS, 2);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0_check_env.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--container_env", Constants.TEST_TASK_EXECUTOR_NUM_HB_MISS + "=5",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.worker.instances=1",
    });
    int exitCode = client.start();
    Assert.assertNotEquals(exitCode, 0);
  }

  @Test
  public void testPSSkewedWorkerTrainingShouldPass() throws ParseException, IOException {
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.PS_JOB_NAME), 1);
    conf.setInt(TonyConfigurationKeys.getInstancesKey(Constants.WORKER_JOB_NAME), 2);
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0_check_env.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--container_env", Constants.TEST_TASK_EXECUTOR_SKEW + "=worker#0#30000"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testPSWorkerTrainingShouldPass() throws ParseException, IOException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "python check_env_and_venv.py",
        "--hdfs_classpath", libPath,
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--python_venv", "tony-core/src/test/resources/test.zip",
        "--conf", "tony.worker.instances=1",
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testWorkerTrainingPyTorchShouldPass() throws ParseException, IOException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0_check_pytorchenv.py",
        "--hdfs_classpath", libPath,
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
  public void testPSWorkerTrainingShouldFail() throws ParseException, IOException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_1.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.worker.instances=1",
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testSingleNodeTrainingShouldFail() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_1.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testAMCrashTonyShouldFail() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--container_env", Constants.TEST_AM_CRASH + "=true",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testAMThrowExceptionCrashTonYShouldFail() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--executes", "exit_0.py",
            "--hdfs_classpath", libPath,
            "--python_binary_path", "python",
            "--container_env", Constants.TEST_AM_THROW_EXCEPTION_CRASH + "=true",
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testTonyAMSchedulerShouldPass() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[] {
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--conf", "tony.worker.instances=1",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.db.instances=1",
        "--conf", "tony.dbloader.instances=1",
        "--conf", "tony.application.prepare-stage=dbloader,db",
        "--conf", "tony.application.training-stage=ps,worker"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  /**
   * Test that makes sure if a worker is killed due to OOM, AM should stop the training (or retry).
   * This test might hang if there is a regression in handling the OOM scenario.
   *
   * The reason why we use a Constants.TEST_WORKER_TERMINATED flag instead of simply requesting more memory than
   * allocated is that Physical Memory Enforcement doesn't seem to work under MiniYARN.
   */
  @Test
  public void testAMStopsJobAfterWorker0Killed() throws ParseException, IOException {
    client.init(new String[]{"--src_dir", "tony-core/src/test/resources/scripts", "--executes", "exit_0.py",
        "--hdfs_classpath", libPath, "--python_binary_path", "python", "--container_env",
        Constants.TEST_WORKER_TERMINATED + "=true", "--conf", "tony.worker.instances=1"});
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  /**
   * Test amRpcClient is nulled out after client finishes.
   */
  @Test
  public void testNullAMRpcClient() throws ParseException, IOException {
    String[] args = new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python"
    };
    client.init(args);
    Assert.assertTrue(client.start() == 0);
    Assert.assertNull(client.getAMRpcClient());
  }

  @Test
  public void testNonChiefWorkerFail() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_1.py",
        "--hdfs_classpath", libPath,
        "--python_binary_path", "python",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.worker.instances=1"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  @Test
  public void testTonyResourcesFlag() throws ParseException, IOException {
    client = new TonyClient(conf);
    String resources = "tony-core/src/test/resources/test.zip::test20.zip"
        + ",tony-core/src/test/resources/test2.zip#archive,"
        + ",tony-core/src/test/resources/common.zip,"
        + libPath;
    client.init(new String[]{
        "--executes", "python check_archive_file_localization.py",
        "--hdfs_classpath", libPath,
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--conf", "tony.worker.instances=1",
        "--conf", "tony.worker.resources=" + resources,
        "--conf", "tony.ps.instances=0",
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testTensorBoardPortSetOnlyOnChiefWorker() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "python check_tb_port_set_in_chief_only.py",
        "--hdfs_classpath", libPath,
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--conf", "tony.chief.instances=1",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.worker.instances=1",
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testStandaloneRuntimeModeShouldPass() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--executes", "exit_0.py",
            "--hdfs_classpath", libPath,
            "--python_binary_path", "python",
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--conf", "tony.application.framework=standalone",
            "--conf", "tony.worker.instances=1"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testStandaloneRuntimeWithMultiInstancesShouldFail() throws ParseException, IOException {
    client = new TonyClient(conf);
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--executes", "exit_0.py",
            "--hdfs_classpath", libPath,
            "--python_binary_path", "python",
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--conf", "tony.application.framework=standalone",
            "--conf", "tony.worker.instances=1",
            "--conf", "tony.slave.instances=1"
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
  }

  /**
   * Tests that when the task completion notification is delayed (RMCallbackHandler.onContainersCompleted),
   * the task has already been unregistered from the heartbeat monitor and thus the job should still succeed.
   */
  @Test
  public void testTaskCompletionNotificationDelayed() throws IOException, ParseException {
    client = new TonyClient(conf);
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--executes", "exit_0.py",
        "--python_binary_path", "python",
        "--hdfs_classpath", libPath,
        "--conf", "tony.ps.instances=0",
        "--conf", "tony.worker.instances=1",
        "--conf", TASK_HEARTBEAT_INTERVAL_MS + "=100",
        "--conf", TASK_MAX_MISSED_HEARTBEATS + "=5",
        "--container_env", Constants.TEST_TASK_COMPLETION_NOTIFICATION_DELAYED + "=true",
    });
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
  }

  @Test
  public void testTonyClientCallbackHandler() throws IOException, ParseException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--hdfs_classpath", libPath,
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--python_venv", "tony-core/src/test/resources/test.zip",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.worker.instances=1",
        "--conf", "tony.ps.command=python sleep_30.py",
        "--conf", "tony.worker.command=python check_env_and_venv.py",
    });
    client.addListener(handler);
    int exitCode = client.start();
    Set<String> expectedJobs = new HashSet<>();
    Set<String> actualJobs = new HashSet<>();
    expectedJobs.add(Constants.WORKER_JOB_NAME);
    expectedJobs.add(Constants.PS_JOB_NAME);
    Assert.assertNotNull(handler.appId);
    Assert.assertEquals(exitCode, 0);
    client.removeListener(handler);
    Assert.assertEquals(handler.getTaskInfoSet().size(), 2);
    for (TaskInfo taskInfo : handler.getTaskInfoSet()) {
      String name = taskInfo.getName();
      TaskStatus status = taskInfo.getStatus();
      actualJobs.add(name);
      if (name.equals(Constants.WORKER_JOB_NAME)) {
        Assert.assertEquals(status, TaskStatus.SUCCEEDED);
      } else {
        Assert.assertEquals(taskInfo.getStatus(), TaskStatus.FINISHED);
      }
    }
    Assert.assertNotNull(handler.getAppId());
    Assert.assertEquals(actualJobs, expectedJobs);
  }

  @Test
  public void testTonyPSCrashShouldFailAndStopAM() throws IOException, ParseException {
    client.init(new String[]{
        "--src_dir", "tony-core/src/test/resources/scripts",
        "--hdfs_classpath", libPath,
        "--shell_env", "ENV_CHECK=ENV_CHECK",
        "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
        "--python_venv", "tony-core/src/test/resources/test.zip",
        "--conf", "tony.ps.instances=1",
        "--conf", "tony.worker.instances=1",
        "--conf", "tony.ps.command=python exit_1.py",
        "--conf", "tony.worker.command=python sleep_30.py",
        "--conf", "tony.application.untracked.jobtypes=ps"
      });
    client.addListener(handler);
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
    client.removeListener(handler);
    Assert.assertEquals(handler.getTaskInfoSet().size(), 2);
    for (TaskInfo taskInfo : handler.getTaskInfoSet()) {
      String name = taskInfo.getName();
      // Workers should be killed by the AM, so they should end up in FINISHED state.
      if (name.equals(Constants.WORKER_JOB_NAME)) {
        Assert.assertEquals(taskInfo.getStatus(), TaskStatus.FINISHED);
      }
      if (name.equals(Constants.PS_JOB_NAME)) {
        Assert.assertEquals(taskInfo.getStatus(), TaskStatus.FAILED);
      }
    }
    Assert.assertNotNull(handler.getAppId());
  }

  @Test
  public void testTonySidecarExecutorCrashShouldPass() throws IOException, ParseException {
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--hdfs_classpath", libPath,
            "--shell_env", "ENV_CHECK=ENV_CHECK",
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--python_venv", "tony-core/src/test/resources/test.zip",
            "--conf", "tony.sidecarexecutor.instances=1",
            "--conf", "tony.worker.instances=1",
            "--conf", "tony.sidecarexecutor.command=python exit_1.py",
            "--conf", "tony.worker.command=python sleep_30.py",
            "--conf", "tony.application.sidecar.jobtypes=sidecarexecutor"
    });
    client.addListener(handler);
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
    client.removeListener(handler);
    Assert.assertEquals(handler.getTaskInfoSet().size(), 2);
    for (TaskInfo taskInfo : handler.getTaskInfoSet()) {
      String name = taskInfo.getName();
      // Workers should be killed by the AM, so they should end up in FINISHED state.
      if (name.equals(Constants.WORKER_JOB_NAME)) {
        Assert.assertEquals(taskInfo.getStatus(), TaskStatus.SUCCEEDED);
      }
      if (name.equals("sidecarexecutor")) {
        Assert.assertEquals(taskInfo.getStatus(), TaskStatus.FAILED);
      }
    }
    Assert.assertNotNull(handler.getAppId());
  }

  @Test
  public void testTonyHorovodDriverCrashShouldFailAndStopAM() throws ParseException, IOException {
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--hdfs_classpath", libPath,
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--python_venv", "tony-core/src/test/resources/test.zip",
            "--conf", "tony.worker.instances=1",
            "--conf", "tony.worker.command=python sleep_30.py",
            "--conf", "tony.horovod.mode.test.fast.fail=true",
            "--conf", "tony.application.framework=horovod"
    });
    client.addListener(handler);
    int exitCode = client.start();
    Assert.assertEquals(exitCode, -1);
    client.removeListener(handler);
  }

  @Test
  public void testTonyHorovodShouldPass() throws ParseException, IOException {
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--hdfs_classpath", libPath,
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--python_venv", "tony-core/src/test/resources/test.zip",
            "--executes", "python check_horovod_env.py",
            "--conf", "tony.worker.instances=2",
            "--conf", "tony.horovod.mode.test=true",
            "--conf", "tony.application.framework=horovod"
    });
    client.addListener(handler);
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
    client.removeListener(handler);
  }

  @Test
  public void testTonyHorovodWithDebugModeShouldPass() throws ParseException, IOException {
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--hdfs_classpath", libPath,
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--python_venv", "tony-core/src/test/resources/test.zip",
            "--executes", "python check_horovod_env.py",
            "--conf", "tony.application.framework=horovod",
            "--conf", "tony.horovod.mode.test=true",
            "--conf", "tony.horovod.driver.mode.debug=true",
            "--conf", "tony.worker.instances=2",
            "--conf", "tony.driver.instances=1",
            "--conf", "tony.driver.vcores=1",
            "--conf", "tony.application.untracked.jobtypes=driver",
            "--conf", "tony.driver.command=python horovod_debug_driver.py -t -p 9999"
    });
    client.addListener(handler);
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
    client.removeListener(handler);
  }

  /**
   * When enable the sidecar tensorboard, it will start the sidecar executor(tensorboard role).
   */
  @Test
  public void testAttachedTensorboardShouldPass() throws ParseException, IOException {
    client.init(new String[]{
            "--src_dir", "tony-core/src/test/resources/scripts",
            "--hdfs_classpath", libPath,
            "--container_env", Constants.SKIP_HADOOP_PATH + "=true",
            "--python_venv", "tony-core/src/test/resources/test.zip",
            "--executes", "python check_tb_port_no_set_in_chief.py",
            "--conf", "tony.chief.instances=1",
            "--conf", "tony.ps.instances=1",
            "--conf", "tony.worker.instances=2",
            "--conf", "tony.application.tensorboard-log-dir=/tmp",
            "--conf", "tony.application.framework=tensorflow",
            "--container_env", Constants.SIDECAR_TB_TEST_KEY + "=true"
    });
    client.addListener(handler);
    int exitCode = client.start();
    Assert.assertEquals(exitCode, 0);
    client.removeListener(handler);
  }

  /**
   * Since we are switching from passing arguments to ApplicationMaster & TaskExecutor
   * to passing tony configuration file. It is critical to make sure all fields in
   * TonyConfFinal.xml is properly set up.
   * Adding a full e2e TestTonyE2E is heavy, this function serves as a simplified lightweight
   * place to make sure TonyConfFinal is set correctly.
   */
  @Test
  public void testTonyFinalConf() throws IOException, YarnException, ParseException,
      InterruptedException, URISyntaxException {
    TonyClient client = spy(this.client);
    client.init(new String[]{
        "--executes", "ls",
        "--shell_env", "TEST1=test",
        "--container_env", "TEST2=test",
        "--conf", "tony.worker.command=cat",
        "--conf", "tony.containers.resources=tony-core/src/test/resources/test.zip"
    });
    // Stub actual app submission logic
    doReturn(true).when(client).monitorApplication();
    doNothing().when(client).submitApplication(any());
    client.start();
    String path = client.processFinalTonyConf();
    Configuration finalConf = new Configuration();
    finalConf.addResource(new Path(path));
    assertEquals(finalConf.get(TonyConfigurationKeys.getContainerExecuteCommandKey()), "ls");
    assertEquals(finalConf.get(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV), "TEST2=test");
    assertEquals(finalConf.get(TonyConfigurationKeys.EXECUTION_ENV), "TEST1=test");
    assertEquals(finalConf.get(TonyConfigurationKeys.getExecuteCommandKey("worker")), "cat");

    // command line arguments should not override tony conf file for values that could have multiple values.
    assertTrue(finalConf.get(TonyConfigurationKeys.getContainerResourcesKey()).contains("test.zip"));
    assertTrue(finalConf.get(TonyConfigurationKeys.getContainerResourcesKey()).contains("common.zip"));
  }

  /**
   * Adding a full e2e TestTonyE2E is heavy, this function serves as a simplified lightweight
   * place to make sure TonyConfFinal is set correctly and does not throw a FileNotFoundException when there are
   * non existent HDFS class paths.
   */
  @Test
  public void testTonyFinalConfWithNonExistentHDFSClasspath() throws IOException, YarnException, ParseException,
                                         InterruptedException, URISyntaxException {
    TonyClient client = spy(this.client);
    String missingClasspath = cluster.getHdfsConf().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY) + "/random/path";
    client.init(new String[]{
        "--executes", "ls",
        "--hdfs_classpath", missingClasspath + "," + libPath,
        "--shell_env", "TEST1=test",
        "--container_env", "TEST2=test",
        "--conf", "tony.worker.command=cat",
        "--conf", "tony.containers.resources=tony-core/src/test/resources/test.zip"
    });
    // Stub actual app submission logic
    doReturn(true).when(client).monitorApplication();
    doNothing().when(client).submitApplication(any());
    client.start();
    String path = client.processFinalTonyConf();
    Configuration finalConf = new Configuration();
    finalConf.addResource(new Path(path));
    // Conf should contain the path that exists
    assertTrue(finalConf.get(TonyConfigurationKeys.getContainerResourcesKey()).contains("yarn/lib"));
    // Conf should not contain the path that does not exist
    assertFalse(finalConf.get(TonyConfigurationKeys.getContainerResourcesKey()).contains("random/path"));
  }

}
