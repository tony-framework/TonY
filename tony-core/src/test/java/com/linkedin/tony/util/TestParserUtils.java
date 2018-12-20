/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.Constants;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.models.JobMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class TestParserUtils {
  private static FileSystem fs = null;

  @BeforeClass
  public static void setup() {
    HdfsConfiguration conf = new HdfsConfiguration();
    try {
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      fail("Failed setting up FileSystem object");
    }
  }

  @Test
  public void testIsValidHistFileName_true() {
    String fileName = "job123-1-1-user1-FAILED.jhist";
    String jobRegex = "job\\d+";

    assertTrue(ParserUtils.isValidHistFileName(fileName, jobRegex));
  }

  @Test
  public void testIsValidHistFileName_false() {
    // Job name doesn't match job regex
    String fileName1 = "application123-1-1-user1-FAILED.jhist";
    // User isn't supposed to be upper-cased
    String fileName2 = "job123-1-1-USER-SUCCEEDED.jhist";
    String jobRegex = "job\\d+";

    assertFalse(ParserUtils.isValidHistFileName(fileName1, jobRegex));
    assertFalse(ParserUtils.isValidHistFileName(fileName2, jobRegex));
  }

  @Test
  public void testParseMetadata_success() {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    String jobRegex = "application\\d+";
    String RMLink = Utils.buildRMUrl(new YarnConfiguration(), "application123");
    JobMetadata expected = new JobMetadata("application123", "/" + Constants.JOBS_SUFFIX + "/application123",
        "/" + Constants.CONFIG_SUFFIX + "/application123", RMLink,1, 1, "SUCCEEDED", "user1");
    JobMetadata actual = ParserUtils.parseMetadata(fs, jobFolder, jobRegex);

    assertEquals(actual.getId(), expected.getId());
    assertEquals(actual.getJobLink(), expected.getJobLink());
    assertEquals(actual.getConfigLink(), expected.getConfigLink());
    assertEquals(actual.getRMLink(), expected.getRMLink());
    assertEquals(actual.getStartedDate(), expected.getStartedDate());
    assertEquals(actual.getCompletedDate(), expected.getCompletedDate());
    assertEquals(actual.getStatus(), expected.getStatus());
    assertEquals(actual.getUser(), expected.getUser());
  }

  @Test
  public void testParseMetadata_fail_IOException() throws IOException {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    String jobRegex = "application\\d+";
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listStatus(jobFolder)).thenThrow(new IOException("IO Excpt"));

    JobMetadata result = ParserUtils.parseMetadata(mockFs, jobFolder, jobRegex);
    assertNull(result);
  }

  @Test
  public void testParseConfig_success() {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    List<JobConfig> expected = new ArrayList<>();
    JobConfig expectedConfig = new JobConfig();
    expectedConfig.setName("name");
    expectedConfig.setValue("value");
    expectedConfig.setFinal(true);
    expectedConfig.setSource("source");

    expected.add(expectedConfig);
    List<JobConfig> actual = ParserUtils.parseConfig(fs, jobFolder);

    assertEquals(actual.size(), expected.size());
    assertEquals(actual.get(0).getName(), expected.get(0).getName());
    assertEquals(actual.get(0).getValue(), expected.get(0).getValue());
    assertEquals(actual.get(0).isFinal(), expected.get(0).isFinal());
    assertEquals(actual.get(0).getSource(), expected.get(0).getSource());
  }

  @Test
  public void testParseConfig_fail_IOException() throws IOException {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listStatus(jobFolder)).thenThrow(new IOException("IO Excpt"));

    List<JobConfig> loc = ParserUtils.parseConfig(mockFs, jobFolder);
    assertEquals(0, loc.size());
  }
}
