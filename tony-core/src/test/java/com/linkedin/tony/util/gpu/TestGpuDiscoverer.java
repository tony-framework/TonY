/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.tony.util.gpu;

import com.linkedin.tony.TonyConfigurationKeys;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;



/**
 * Ported from Hadoop 2.9.0
 */
public class TestGpuDiscoverer {
  private String getTestParentFolder() {
    File f = new File("target/temp/" + TestGpuDiscoverer.class.getName());
    return f.getAbsolutePath();
  }

  private void touchFile(File f) throws IOException {
    new FileOutputStream(f).close();
  }

  @BeforeTest
  public void before() throws IOException {
    String folder = getTestParentFolder();
    File f = new File(folder);
    FileUtils.deleteDirectory(f);
    if (!f.mkdirs()) {
      System.err.println("Cannot make directory");
    }
  }

  @Test
  public void testLinuxGpuResourceDiscoverPluginConfig() throws Exception {
    // Only run this on demand.
    if (!Boolean.valueOf(
        System.getProperty("RunLinuxGpuResourceDiscoverPluginConfigTest"))) {
      throw new SkipException("Skip this");
    }

    // test case 1, check default setting.
    Configuration conf = new Configuration(false);
    conf.set(TonyConfigurationKeys.GPU_PATH_TO_EXEC, TonyConfigurationKeys.DEFAULT_GPU_PATH_TO_EXEC);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    Assert.assertEquals(GpuDiscoverer.DEFAULT_BINARY_NAME,
        plugin.getPathOfGpuBinary());
    Assert.assertNotNull(plugin.getEnvironmentToRunCommand().get("PATH"));
    Assert.assertTrue(
        plugin.getEnvironmentToRunCommand().get("PATH").contains("nvidia"));

    // test case 2, check mandatory set path.
    File fakeBinary = new File(getTestParentFolder(),
        GpuDiscoverer.DEFAULT_BINARY_NAME);
    touchFile(fakeBinary);
    conf.set(TonyConfigurationKeys.GPU_PATH_TO_EXEC, getTestParentFolder());
    plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    Assert.assertEquals(fakeBinary.getAbsolutePath(),
        plugin.getPathOfGpuBinary());
    Assert.assertNull(plugin.getEnvironmentToRunCommand().get("PATH"));

    // test case 3, check mandatory set path, but binary doesn't exist so default
    // path will be used.
    if (!fakeBinary.delete()) {
      System.err.println("Cannot delete binary");
    }
    plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    Assert.assertEquals(GpuDiscoverer.DEFAULT_BINARY_NAME,
        plugin.getPathOfGpuBinary());
    Assert.assertTrue(
        plugin.getEnvironmentToRunCommand().get("PATH").contains("nvidia"));
  }

  @Test
  public void testGpuDiscover() throws GpuInfoException {
    // Since this is more of a performance unit test, only run if
    // RunUserLimitThroughput is set (-DRunUserLimitThroughput=true)
    if (!(Boolean.valueOf(System.getProperty("runGpuDiscoverUnitTest")))) {
      throw new SkipException("Skip this");
    }
    Configuration conf = new Configuration(false);
    GpuDiscoverer plugin = new GpuDiscoverer();
    plugin.initialize(conf);
    GpuDeviceInformation info = plugin.getGpuDeviceInformation();

    Assert.assertTrue(info.getGpus().size() > 0);
  }

}
