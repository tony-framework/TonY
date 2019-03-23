/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TestClusterSubmitter {
  @Test
  public void testClusterSubmitter() throws  Exception {
    TonyClient client = spy(new TonyClient());
    doReturn(0).when(client).start(); // Don't really call start() method.

    ClusterSubmitter submitter = new ClusterSubmitter(client);
    int exitCode = submitter.submit(new String[] {"--src_dir", "src"});
    assertEquals(exitCode, 0);
    assertTrue(
        client.getTonyConf().get(TonyConfigurationKeys.getContainerResourcesKey()).contains(Constants.TONY_JAR_NAME));
  }
}

