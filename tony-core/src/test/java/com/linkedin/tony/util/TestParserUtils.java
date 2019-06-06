/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import com.linkedin.tony.Constants;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.models.JobMetadata;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class TestParserUtils {
  private static final ClassLoader CLASSLOADER = TestParserUtils.class.getClassLoader();

  private static FileSystem fs = null;
  private YarnConfiguration yarnConf = new YarnConfiguration();

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
  public void testIsValidHistFileNameTrue() {
    String fileName = "job123-1-1-user1-FAILED." + Constants.HISTFILE_SUFFIX;
    String jobRegex = "job\\d+";

    assertTrue(ParserUtils.isValidHistFileName(fileName, jobRegex));
  }

  @Test
  public void testIsValidHistFileNameFalse() {
    // Job name doesn't match job regex
    String fileName1 = "application123-1-1-user1-FAILED." + Constants.HISTFILE_SUFFIX;
    // User isn't supposed to be upper-cased
    String fileName2 = "job123-1-1-USER-SUCCEEDED." + Constants.HISTFILE_SUFFIX;
    String jobRegex = "job\\d+";

    assertFalse(ParserUtils.isValidHistFileName(fileName1, jobRegex));
    assertFalse(ParserUtils.isValidHistFileName(fileName2, jobRegex));
  }

  @Test
  public void testParseMetadataSuccess() {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    String jobRegex = "application\\d+";
    JobMetadata expected = new JobMetadata.Builder()
        .setId("application123")
        .setConf(yarnConf)
        .setStarted(1)
        .setCompleted(1)
        .setStatus(Constants.SUCCEEDED)
        .setUser("user1")
        .build();
    JobMetadata actual = ParserUtils.parseMetadata(fs, yarnConf, jobFolder, jobRegex);

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
  public void testParseMetadataFailIOException() throws IOException {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    String jobRegex = "application\\d+";
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listStatus(jobFolder)).thenThrow(new IOException("IO Excpt"));

    JobMetadata result = ParserUtils.parseMetadata(mockFs, yarnConf, jobFolder, jobRegex);
    assertNull(result);
  }

  @Test
  public void testParseConfigSuccess() {
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
  public void testConfigMissingElements() {
    Path jobFolder = new Path(CLASSLOADER.getResource("application_123_456").getFile());
    List<JobConfig> actualConfigs = ParserUtils.parseConfig(fs, jobFolder);
    assertEquals(actualConfigs.size(), 3);
  }

  @Test
  public void testParseConfigFailIOException() throws IOException {
    Path jobFolder = new Path(Constants.TONY_CORE_SRC + "test/resources/typicalHistFolder/job1");
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listStatus(jobFolder)).thenThrow(new IOException("IO Excpt"));

    List<JobConfig> loc = ParserUtils.parseConfig(mockFs, jobFolder);
    assertEquals(0, loc.size());
  }

  @Test
  public void testGetYearMonthDayDirectory() {
    Instant instant = Instant.ofEpochMilli(1559155036000L);
    Date date = Date.from(instant);
    ZoneId utc = ZoneId.of("UTC");
    ZoneId gmt6 = ZoneId.of("GMT+6");
    String expectedUTCDirectoryStr = "2019/05/29";
    String expectedGMT6DirectoryStr = "2019/05/30";

    String actualUTCDirectoryStr = ParserUtils.getYearMonthDayDirectory(date, utc);
    String actualGMT6DirectoryStr = ParserUtils.getYearMonthDayDirectory(date, gmt6);
    assertEquals(actualUTCDirectoryStr, expectedUTCDirectoryStr);
    assertEquals(actualGMT6DirectoryStr, expectedGMT6DirectoryStr);
  }
}
