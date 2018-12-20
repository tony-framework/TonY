/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.TonyJobMetadata;
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
    long started = 1L;
    String user = "user";

    TonyJobMetadata metadata = new TonyJobMetadata.Builder()
        .setId(appId)
        .setStartedTime(started)
        .setUser(user)
        .build();
    String expectedName = "app123-1-user.jhist.inprogress";

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }

  @Test
  public void testGenerateFileName_finishedJob() {
    String appId = "app123";
    long started = 1L;
    long completed = 2L;
    String user = "user";

    TonyJobMetadata metadata = new TonyJobMetadata.Builder()
        .setId(appId)
        .setStartedTime(started)
        .setCompleted(completed)
        .setUser(user)
        .setStatus(true)
        .build();
    String expectedName = "app123-1-2-user-SUCCEEDED.jhist";

    assertEquals(HistoryFileUtils.generateFileName(metadata), expectedName);
  }
}
