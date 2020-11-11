/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.tony.TFConfig;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.tensorflow.JobContainerRequest;
import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.testng.annotations.Test;

import static com.linkedin.tony.Constants.LOGS_SUFFIX;
import static com.linkedin.tony.Constants.JOBS_SUFFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class TestUtils {

  @Test
  public void testParseMemoryString() {
    assertEquals(Utils.parseMemoryString("2g"), "2048");
    assertEquals(Utils.parseMemoryString("2M"), "2");
    assertEquals(Utils.parseMemoryString("3"), "3");
  }

  @Test
  public void testPoll() {
    assertTrue(Utils.poll(() -> true, 1, 1));
    assertFalse(Utils.poll(() -> false, 1, 1));
  }

  @Test
  public void testUnzipArchive() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("test.zip").getFile());
    try {
      Utils.unzipArchive(file.getPath(), "venv/");
      Path unzippedFilePath = Paths.get("venv/123.xml");
      assertTrue(Files.exists(unzippedFilePath));
      Files.deleteIfExists(Paths.get("venv/123.xml"));
      Files.deleteIfExists(Paths.get("venv/"));
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testParseContainerRequests() {
    Configuration conf = new Configuration();
    conf.addResource("tony-default.xml");
    conf.setInt("tony.worker.instances", 3);
    conf.setInt("tony.evaluator.instances", 1);
    conf.setInt("tony.worker.gpus", 1);
    conf.setInt("tony.evaluator.vcores", 2);
    conf.setInt("tony.chief.gpus", 1);
    conf.setInt("tony.db.instances", 1);
    conf.setInt("tony.dbwriter.instances", 1);
    conf.setStrings("tony.application.prepare-stage", "dbwriter, db");
    conf.setStrings("tony.application.untracked.jobtypes", "db");

    Map<String, JobContainerRequest> requests = Utils.parseContainerRequests(conf);
    assertEquals(requests.get("worker").getNumInstances(), 3);
    assertEquals(requests.get("evaluator").getNumInstances(), 1);
    assertEquals(requests.get("worker").getGPU(), 1);
    assertEquals(requests.get("evaluator").getVCores(), 2);
    // Check default value.
    assertEquals(requests.get("worker").getMemory(), 2048);
    // Check job does not exist if no instances are configured.
    assertFalse(requests.containsKey("chief"));
    assertEquals(requests.get("worker").getDependsOn(), new ArrayList<>(Arrays.asList("dbwriter")));
    assertEquals(requests.get("evaluator").getDependsOn(), new ArrayList<>(Arrays.asList("dbwriter")));
    assertEquals(requests.get("db").getDependsOn(), new ArrayList<>());
    assertEquals(requests.get("dbwriter").getDependsOn(), new ArrayList<>());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseContainerRequestsShouldFail() {
    Configuration conf = new Configuration();
    conf.addResource("tony-default.xml");
    conf.setInt("tony.worker.instances", 3);
    conf.setInt("tony.evaluator.instances", 1);
    conf.setInt("tony.worker.gpus", 1);
    conf.setInt("tony.evaluator.vcores", 2);
    conf.setInt("tony.chief.gpus", 1);
    conf.setInt("tony.db.instances", 1);
    conf.setInt("tony.dbwriter.instances", 1);
    conf.setStrings("tony.application.prepare-stage", "dbwriter,db");
    conf.setStrings("tony.application.untracked.jobtypes", "db");
    conf.setStrings("tony.application.training-stage", "chief, evaluator, worker");

    Utils.parseContainerRequests(conf);
  }

  @Test
  public void testIsArchive() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file1 = new File(classLoader.getResource("test.zip").getFile());
    File file2 = new File(classLoader.getResource("test.tar").getFile());
    File file3 = new File(classLoader.getResource("test.tar.gz").getFile());
    assertTrue(Utils.isArchive(file1.getAbsolutePath()));
    assertTrue(Utils.isArchive(file2.getAbsolutePath()));
    assertTrue(Utils.isArchive(file3.getAbsolutePath()));
  }

  @Test
  public void testIsNotArchive() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file1 = new File(classLoader.getResource("scripts/exit_0.py").getFile());
    assertFalse(Utils.isArchive(file1.getAbsolutePath()));
  }

  @Test
  public void testRenameFile() throws IOException {
    File tempFile = File.createTempFile("testRenameFile-", "-suffix");
    tempFile.deleteOnExit();
    boolean result = Utils.renameFile(tempFile.getAbsolutePath(),
            tempFile.getAbsolutePath() + "bak");
    assertTrue(Files.exists(Paths.get(tempFile.getAbsolutePath() + "bak")));
    assertTrue(result);
    Files.deleteIfExists(Paths.get(tempFile.getAbsolutePath() + "bak"));
  }

  @Test
  public void testConstructTFConfig() throws IOException {
    String spec = "{\"worker\":[\"host0:1234\", \"host1:1234\"], \"ps\":[\"host2:1234\"]}";
    String tfConfig = Utils.constructTFConfig(spec, "worker", 1);
    ObjectMapper mapper = new ObjectMapper();
    TFConfig config = mapper.readValue(tfConfig, new TypeReference<TFConfig>() { });
    assertEquals(config.getTask().getType(), "worker");
    assertEquals(config.getTask().getIndex(), 1);
    assertEquals(config.getCluster().get("worker").get(0), "host0:1234");
    assertEquals(config.getCluster().get("worker").get(1), "host1:1234");
    assertEquals(config.getCluster().get("ps").get(0), "host2:1234");
  }

  @Test
  public void testBuildRMUrl() {
    Configuration yarnConf = mock(Configuration.class);
    when(yarnConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS)).thenReturn("testrmaddress");
    String expected = "http://testrmaddress/cluster/app/1";
    assertEquals(Utils.buildRMUrl(yarnConf, "1"), expected);
  }

  @Test
  public void testPollTillNonNull() {
    assertNull(Utils.pollTillNonNull(() -> null, 1, 1));
    assertTrue(Utils.pollTillNonNull(() -> true, 1, 1));
  }

  @Test
  public void testConstructUrl() {
    assertEquals(Utils.constructUrl("foobar"), "http://foobar");
    assertEquals(Utils.constructUrl("http://foobar"), "http://foobar");
  }

  @Test
  public void testConstructContainerUrl() {
    Container container = mock(Container.class);
    assertNotNull(Utils.constructContainerUrl(container));
    assertNotNull(Utils.constructContainerUrl("foo", null));
  }

  @Test
  public void testParseKeyValue() {
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put("bar", "");
    hashMap.put("foo", "1");
    hashMap.put("baz", "3");

    assertEquals(Utils.parseKeyValue(null), new HashMap<>());
    assertEquals(Utils.parseKeyValue(
            new String[]{"foo=1", "bar", "baz=3"}), hashMap);
  }

  @Test
  public void testExecuteShell() throws IOException, InterruptedException {
    assertEquals(Utils.executeShell("foo", 0, null), 127);
  }

  @Test
  public void testGetCurrentHostName() {
    assertNull(Utils.getCurrentHostName());
  }

  @Test
  public void testGetHostNameOrIpFromTokenConf()
          throws SocketException, YarnException {
    Configuration conf = mock(Configuration.class);
    when(conf.getBoolean(
            CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
            CommonConfigurationKeys
                    .HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT))
            .thenReturn(false);
    assertNull(Utils.getHostNameOrIpFromTokenConf(conf));
  }

  @Test
  public void testGetAllJobTypes() {
    Configuration conf = new Configuration();
    conf.addResource("tony-default.xml");
    conf.setInt("tony.worker.instances", 3);
    conf.setInt("tony.evaluator.instances", 1);
    conf.setInt("tony.worker.gpus", 1);
    conf.setInt("tony.evaluator.vcores", 2);
    conf.setInt("tony.chief.gpus", 1);

    assertEquals(Utils.getAllJobTypes(conf),
            new HashSet(Arrays.asList("worker", "evaluator")));
  }

  @Test
  public void testGetNumTotalTasks() {
    Configuration conf = new Configuration();
    conf.addResource("tony-default.xml");
    conf.setInt("tony.worker.instances", 3);
    conf.setInt("tony.evaluator.instances", 1);
    conf.setInt("tony.worker.gpus", 1);
    conf.setInt("tony.evaluator.vcores", 2);
    conf.setInt("tony.chief.gpus", 1);

    assertEquals(Utils.getNumTotalTasks(conf), 4);
  }

  @Test
  public void testGetTaskType() {
    assertNull(Utils.getTaskType("foo"));
    assertEquals(Utils.getTaskType("tony.evaluator.instances"),
            "evaluator");
  }

  @Test
  public void testGetClientResourcesPath() {
    assertEquals(Utils.getClientResourcesPath("foo", "bar"),
            "foo-bar");
  }

  @Test
  public void testGetUntrackedJobTypes() {
    Configuration conf = new Configuration();
    conf.addResource("tony-default.xml");
    conf.setInt("tony.worker.instances", 3);
    conf.setInt("tony.evaluator.instances", 1);
    conf.setInt("tony.worker.gpus", 1);
    conf.setInt("tony.evaluator.vcores", 2);
    conf.setInt("tony.chief.gpus", 1);

    assertEquals(Utils.getUntrackedJobTypes(conf),
            new String[]{"ps"}, "Arrays do not match");
  }

  @Test
  public void testIsJobTypeTracked() {
    Configuration conf = new Configuration();
    conf.addResource("tony-default.xml");
    conf.setInt("tony.worker.instances", 3);
    conf.setInt("tony.evaluator.instances", 1);
    conf.setInt("tony.worker.gpus", 1);
    conf.setInt("tony.evaluator.vcores", 2);
    conf.setInt("tony.chief.gpus", 1);

    assertTrue(Utils.isJobTypeTracked("tony.worker.gpus", conf));
  }

  @Test
  public void testGetContainerEnvForDocker() {
    Configuration conf = mock(Configuration.class);
    when(conf.getBoolean(TonyConfigurationKeys.DOCKER_ENABLED,
            TonyConfigurationKeys.DEFAULT_DOCKER_ENABLED))
            .thenReturn(true);
    assertEquals(Utils.getContainerEnvForDocker(conf, "tony.worker.gpus"),
            new HashMap<>());

    when(conf.get(TonyConfigurationKeys
            .getDockerImageKey("tony.worker.gpus"))).thenReturn("foo");
    assertEquals(Utils.getContainerEnvForDocker(conf, "tony.worker.gpus"),
        new HashMap<String, String>() {{
          put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
          put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", "foo");
        }});
  }

  @Test
  public void testLinksToBeDisplayedOnPage() {
    assertEquals(Utils.linksToBeDisplayedOnPage(null), new TreeMap<>());
    Map<String, String> linksToBeDisplayed = Utils.linksToBeDisplayedOnPage("fakeJobId");
    assertEquals(linksToBeDisplayed.size(), 2);
    assertEquals(linksToBeDisplayed.get("Logs"), "/" + LOGS_SUFFIX + "/" + "fakeJobId");
    assertEquals(linksToBeDisplayed.get("Events"), "/" + JOBS_SUFFIX + "/" + "fakeJobId");
  }
}
