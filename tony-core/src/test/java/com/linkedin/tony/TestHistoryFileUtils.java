/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.util.HistoryFileUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestHistoryFileUtils {
  @Test
  public void testGenerateFileName_defaultConstructor() {
    TonyJobMetadata metadata = new TonyJobMetadata();
    String expectedName = "-0-0--.jhist";

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }

  @Test
  public void testGenerateFileName_inprogressJob() {
    String appId = "app123";
    Long started = (long) 1;
    String user = "user";

    TonyJobMetadata metadata =
        TonyJobMetadata.newInstance(new Configuration(), appId, started, null, null, user);
    String expectedName = "app123-1-user.jhist.inprogress";

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }

  @Test
  public void testGenerateFileName_finishedJob() {
    String appId = "app123";
    Long started = (long) 1;
    Long completed = (long) 2;
    String user = "user";
    Boolean status = true;

    TonyJobMetadata metadata =
        TonyJobMetadata.newInstance(new Configuration(), appId, started, completed, status, user);
    String expectedName = "app123-1-2-user-SUCCEEDED.jhist";

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }
}
