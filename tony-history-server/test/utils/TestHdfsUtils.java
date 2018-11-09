package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
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
  public void testPathExists_true() {
    Path exists = new Path("./test/resources/file.txt");

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
    Path filePath = new Path("./test/resources/file.txt");

    assertEquals("someContent", HdfsUtils.contentOfHdfsFile(fs, filePath));
  }

  @Test
  public void testContentOfHdfsFile_noContent() {
    Path filePath = new Path("./test/resources/empty.txt");

    assertEquals("", HdfsUtils.contentOfHdfsFile(fs, filePath));
  }

  @Test
  public void testContentOfHdfsFile_throwsException() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path filePath = new Path("./test/resources/empty.txt");

    when(mockFs.exists(filePath)).thenThrow(new IOException("IO Excpt"));

    assertEquals("", HdfsUtils.contentOfHdfsFile(fs, filePath));
  }

  @Test
  public void testGetMetadataFilePaths_emptyHistoryFolder() {
    String histFolder = "./test/resources/emptyHistFolder";

    assertEquals(new ArrayList<Path>(), HdfsUtils.getMetadataFilePaths(fs, histFolder));
  }

  @Test
  public void testGetMetadataFilePaths_typicalHistoryFolder() {
    String histFolder = "./test/resources/typicalHistFolder";
    List<Path> expectedRes = new ArrayList<>();
    StringBuilder sb = new StringBuilder();

    for (int i = 1; i < 6; ++i) {
      sb.append("file:").append(System.getProperty("user.dir"));
      sb.append("/test/resources/typicalHistFolder/job").append(i);
      sb.append("/metadata.json");
      expectedRes.add(new Path(sb.toString()));
      sb = new StringBuilder();
    }
    List<Path> actualRes = HdfsUtils.getMetadataFilePaths(fs, histFolder);
    Collections.sort(actualRes);

    assertEquals(expectedRes, actualRes);
    assertEquals(expectedRes.size(), actualRes.size());
  }

  @Test
  public void testGetMetadataFilePaths_throwsException() throws IOException {
    String histFolder = "./test/resources/typicalHistFolder";
    FileSystem mockFs = mock(FileSystem.class);

    when(mockFs.listStatus(new Path(histFolder))).thenThrow(new IOException("IO Excpt"));
    List<Path> actualRes = HdfsUtils.getMetadataFilePaths(mockFs, histFolder);

    assertEquals(new ArrayList<Path>(), actualRes);
    assertEquals(0, actualRes.size());
  }
}
