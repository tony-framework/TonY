/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.cli.ClusterSubmitter;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class TestClusterSubmitter {

  @Test
  public void testClusterSubmmiter() throws  Exception {
    TonyClient client = spy(new TonyClient());
    doReturn(0).when(client).start(); // Don't really call start() method.

    ClusterSubmitter submitter = new ClusterSubmitter(client);
    int exitCode = submitter.submit(new String[] {"--src_dir", "src"});
    assertEquals(exitCode, 0);
  }

}

