/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTonyClient {

  TonyClient client;

  @BeforeClass
  public void setup() {
    client = new TonyClient();
  }

  @Test
  public void testCreateAMContainerSpec() throws Exception {
    File zipFile = new File(System.getProperty("user.dir") + "/tony_archive.zip");
    zipFile.createNewFile();
    zipFile.deleteOnExit();
    File tonyConfFile = new File(System.getProperty("user.dir")
        + File.separator + Constants.TONY_FINAL_XML);
    tonyConfFile.createNewFile();
    tonyConfFile.deleteOnExit();
    client.createYarnClient();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ContainerLaunchContext clc = client.createAMContainerSpec(appId,
        8192, null, null, null, null, getTokens(), null);
    List<String> cmds = clc.getCommands();
    Assert.assertTrue(cmds.get(0).contains("-Xmx6553m"));
  }

  private ByteBuffer getTokens() throws IOException {
    Credentials creds = new Credentials();
    DataOutputBuffer buffer = new DataOutputBuffer();
    creds.writeTokenStorageToStream(buffer);
    return ByteBuffer.wrap(buffer.getData(), 0, buffer.getLength());
  }
}
