/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.util.HdfsUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.mockito.Mockito.*;


public class TestHdfsUtils {
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
  public void testScanDir_emptyDir() {
    Path histFolder = new Path("./tony-core/src/test/resources/emptyHistFolder");

    assertEquals(HdfsUtils.scanDir(fs, histFolder).length, 0);
  }

  @Test
  public void testScanDir_typical() {
    Path histFolder = new Path("./tony-core/src/test/resources/typicalHistFolder");
    FileStatus[] res = HdfsUtils.scanDir(fs, histFolder);
    assertEquals(res.length, 5);
  }

  @Test
  public void testScanDir_nullDir() {
    assertEquals(HdfsUtils.scanDir(fs, null).length, 0);
  }

  @Test
  public void testScanDir_throwsException() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path invalidPath = new Path("/invalid/path");

    when(mockFs.listStatus(invalidPath)).thenThrow(new IOException("IO Excpt"));

    assertEquals(HdfsUtils.scanDir(mockFs, invalidPath).length, 0);
    verify(mockFs).listStatus(invalidPath);
  }

  @Test
  public void testPathExists_true() {
    Path exists = new Path("./tony-core/src/test/resources/file.txt");

    assertTrue(HdfsUtils.pathExists(fs, exists));
  }

  @Test
  public void testPathExists_false() {
    Path invalidPath = new Path("/invalid/path");

    assertFalse(HdfsUtils.pathExists(fs, invalidPath));
  }

  @Test
  public void testPathExists_throwsException() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path invalidPath = new Path("/invalid/path");

    when(mockFs.exists(invalidPath)).thenThrow(new IOException("IO Excpt"));

    assertFalse(HdfsUtils.pathExists(mockFs, invalidPath));
  }

  @Test
  public void testContentOfHdfsFile_withContent() {
    Path filePath = new Path("./tony-core/src/test/resources/file.txt");

    assertEquals(HdfsUtils.contentOfHdfsFile(fs, filePath),"someContent");
  }

  @Test
  public void testContentOfHdfsFile_noContent() {
    Path filePath = new Path("./tony-core/src/test/resources/empty.txt");

    assertEquals(HdfsUtils.contentOfHdfsFile(fs, filePath), "");
  }

  @Test
  public void testContentOfHdfsFile_throwsException() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path filePath = new Path("./tony-core/src/test/resources/empty.txt");

    when(mockFs.exists(filePath)).thenThrow(new IOException("IO Excpt"));

    assertEquals(HdfsUtils.contentOfHdfsFile(fs, filePath), "");
  }

  @Test
  public void testGetJobId_typicalCase() {
    Path filePath1 = new Path("./tony-core/src/test/resources/job1");
    Path filePath2 = new Path("./tony-core/src/test/resources/app2/");

    assertEquals("job1", HdfsUtils.getJobId(filePath1.toString()));
    assertEquals("app2", HdfsUtils.getJobId(filePath2.toString()));
  }

  @Test
  public void testGetJobId_emptyPath() {
    Path filePath = mock(Path.class);
    when(filePath.toString()).thenReturn("");

    assertEquals(HdfsUtils.getJobId(filePath.toString()), "");
  }

  @Test
  public void testIsJobFolder_match() {
    Path filePath1 = new Path("/abc/def/application_1541469337545_0024");
    Path filePath2 = new Path("/abc/def/job2");
    String regex1 = "^application_.*";
    String regex2 = "^job.*";

    assertTrue(HdfsUtils.isJobFolder(filePath1, regex1));
    assertTrue(HdfsUtils.isJobFolder(filePath2, regex2));
  }

  @Test
  public void testIsJobFolder_notMatch() {
    Path filePath1 = new Path("./tony-core/src/test/job/application_1541469337545_0024");
    Path filePath2 = new Path("./tony-core/src/test/resources/application2/");
    String regex1 = ".*job.*";
    String regex2 = "application_.*";

    assertFalse(HdfsUtils.isJobFolder(filePath1, regex1));
    assertFalse(HdfsUtils.isJobFolder(filePath2, regex2));
  }

  @Test
  public void testGetJobFolders_emptyHistoryFolder() {
    Path histFolder = new Path("./tony-core/src/test/resources/emptyHistFolder");

    assertEquals(HdfsUtils.getJobFolders(fs, histFolder, "job*"), new ArrayList<Path>());
  }

  @Test
  public void testGetJobFolders_typicalHistoryFolder() {
    Path histFolder = new Path("./tony-core/src/test/resources/typicalHistFolder");
    String regex = "^job.*";
    List<Path> expectedRes = new ArrayList<>();

    for (int i = 1; i < 6; ++i) {
      StringBuilder sb = new StringBuilder();
      sb.append("file:").append(System.getProperty("user.dir"));
      sb.append("/tony-core/src/test/resources/typicalHistFolder/job").append(i);
      expectedRes.add(new Path(sb.toString()));
    }
    List<Path> actualRes = HdfsUtils.getJobFolders(fs, histFolder, regex);
    Collections.sort(actualRes);

    assertEquals(actualRes, expectedRes);
    assertEquals(actualRes.size(), expectedRes.size());
  }

  @Test
  public void testGetJobFolders_nestedHistFolder() {
    Path histFolder = new Path("./tony-core/src/test/resources/nestedHistFolder");
    String regex = "^job.*";
    List<Path> expectedRes = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    sb.append("file:").append(System.getProperty("user.dir"));
    sb.append("/tony-core/src/test/resources/nestedHistFolder/");
    String baseDir = sb.toString();

    expectedRes.add(new Path(baseDir, "2018/01/02/job0"));
    expectedRes.add(new Path(baseDir, "2018/01/01/job1"));
    expectedRes.add(new Path(baseDir, "2018/12/31/job2"));
    expectedRes.add(new Path(baseDir, "2017/07/job3"));
    expectedRes.add(new Path(baseDir, "2017/07/job4"));
    expectedRes.add(new Path(baseDir, "2017/07/job5"));

    List<Path> actualRes = HdfsUtils.getJobFolders(fs, histFolder, regex);
    Collections.sort(actualRes, (o1, o2) -> {
      String job1 = HdfsUtils.getJobId(o1.toString());
      String job2 = HdfsUtils.getJobId(o2.toString());
      return job1.charAt(job1.length()-1) - job2.charAt(job2.length()-1);
    });

    assertEquals(actualRes, expectedRes);
    assertEquals(actualRes.size(), expectedRes.size());
  }

  @Test
  public void testGetJobFolders_throwsException() throws IOException {
    Path histFolder = new Path("./tony-core/src/test/resources/typicalHistFolder");
    FileSystem mockFs = mock(FileSystem.class);
    String regex = "job*";

    when(mockFs.listStatus(histFolder)).thenThrow(new IOException("IO Excpt"));
    List<Path> actualRes = HdfsUtils.getJobFolders(mockFs, histFolder, regex);

    assertEquals(actualRes.size(), 0);
  }
}
