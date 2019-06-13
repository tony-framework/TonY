/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.tony.TFConfig;
import com.linkedin.tony.tensorflow.TensorFlowContainerRequest;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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

    Map<String, TensorFlowContainerRequest> requests = Utils.parseContainerRequests(conf);
    assertEquals(3, requests.get("worker").getNumInstances());
    assertEquals(1, requests.get("evaluator").getNumInstances());
    assertEquals(1, requests.get("worker").getGPU());
    assertEquals(2, requests.get("evaluator").getVCores());
    // Check default value.
    assertEquals(2048, requests.get("worker").getMemory());
    // Check job does not exist if no instances are configured.
    assertFalse(requests.containsKey("chief"));
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
    assertEquals("worker", config.getTask().getType());
    assertEquals(1, config.getTask().getIndex());
    assertEquals("host0:1234", config.getCluster().get("worker").get(0));
    assertEquals("host1:1234", config.getCluster().get("worker").get(1));
    assertEquals("host2:1234", config.getCluster().get("ps").get(0));
  }

  @Test
  public void testBuildRMUrl() {
    Configuration yarnConf = mock(Configuration.class);
    when(yarnConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS)).thenReturn("testrmaddress");
    String expected = "http://testrmaddress/cluster/app/1";
    assertEquals(Utils.buildRMUrl(yarnConf, "1"), expected);
  }
}
