/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.Constants;
import com.linkedin.tony.models.JobMetadata;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestHistoryFileUtils {
  @Test
  public void testGenerateFileNameInProgressJob() {
    String appId = "app123";
    long started = 1L;
    String user = "user";

    JobMetadata metadata = new JobMetadata.Builder()
        .setId(appId)
        .setStarted(started)
        .setUser(user)
        .setConf(new YarnConfiguration())
        .build();
    String expectedName = "app123-1-user." + Constants.HISTFILE_SUFFIX + "." + Constants.INPROGRESS;

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }

  @Test
  public void testGenerateFileNameFinishedJob() {
    String appId = "app123";
    long started = 1L;
    long completed = 2L;
    String user = "user";

    JobMetadata metadata = new JobMetadata.Builder()
        .setId(appId)
        .setStarted(started)
        .setCompleted(completed)
        .setUser(user)
        .setStatus(Constants.SUCCEEDED)
        .setConf(new YarnConfiguration())
        .build();
    String expectedName = "app123-1-2-user-SUCCEEDED." + Constants.HISTFILE_SUFFIX;

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }
}
