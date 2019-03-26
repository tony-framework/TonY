/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

import com.linkedin.tony.TonyClient;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestNotebookSubmitter {
  @Test
  public void testCallbackHandlerSet() {
    NotebookSubmitter ns = new NotebookSubmitter();
    TonyClient client = ns.getClient();
    Assert.assertTrue(client.getListener().size() > 0);
  }
}
