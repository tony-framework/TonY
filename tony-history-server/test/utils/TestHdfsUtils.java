package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestHdfsUtils {
  private static FileSystem fs = null;

  @BeforeClass
  public static void setup() {
    HdfsConfiguration conf = new HdfsConfiguration();
    try {
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      Assert.fail("Failed setting up FileSystem object");
    }
  }

  @Test
  public void testPathExists_true() {
    Path exists = new Path("./test/resources/file.txt");

    Assert.assertTrue(HdfsUtils.pathExists(fs, exists));
  }

  @Test
  public void testPathExists_false() {
    Path invalidPath = new Path("/invalid/path");

    Assert.assertFalse(HdfsUtils.pathExists(fs, invalidPath));
  }

  @Test
  public void testPathExists_throwsException() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path invalidPath = new Path("/invalid/path");

    when(mockFs.exists(invalidPath)).thenThrow(new IOException("IO Excpt"));

    Assert.assertFalse(HdfsUtils.pathExists(mockFs, invalidPath));
  }

  @Test
  public void testContentOfHdfsFile_withContent() {
    Path filePath = new Path("./test/resources/file.txt");

    Assert.assertEquals("someContent", HdfsUtils.contentOfHdfsFile(fs, filePath));
  }

  @Test
  public void testContentOfHdfsFile_noContent() {
    Path filePath = new Path("./test/resources/empty.txt");

    Assert.assertEquals("", HdfsUtils.contentOfHdfsFile(fs, filePath));
  }

  @Test
  public void testContentOfHdfsFile_throwsException() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path filePath = new Path("./test/resources/empty.txt");

    when(mockFs.exists(filePath)).thenThrow(new IOException("IO Excpt"));

    Assert.assertEquals("", HdfsUtils.contentOfHdfsFile(fs, filePath));
  }

  @Test
  public void testGetMetadataFilePaths_emptyHistoryFolder() {
    String histFolder = "./test/resources/emptyHistFolder";

    Assert.assertEquals(new ArrayList<Path>(), HdfsUtils.getMetadataFilePaths(fs, histFolder));
  }

  @Test
  public void testGetMetadataFilePaths_typicalHistoryFolder() {
    String histFolder = "./test/resources/typicalHistFolder";
    List<Path> expectedRes = new ArrayList<>();

    for (int i = 1; i < 6; ++i) {
      StringBuilder sb = new StringBuilder();
      sb.append("file:").append(System.getProperty("user.dir"));
      sb.append("/test/resources/typicalHistFolder/job").append(i);
      sb.append("/metadata.json");
      expectedRes.add(new Path(sb.toString()));
    }
    List<Path> actualRes = HdfsUtils.getMetadataFilePaths(fs, histFolder);
    Collections.sort(actualRes);

    Assert.assertEquals(expectedRes, actualRes);
    Assert.assertEquals(expectedRes.size(), actualRes.size());
  }

  @Test
  public void testGetMetadataFilePaths_throwsException() throws IOException {
    String histFolder = "./test/resources/typicalHistFolder";
    FileSystem mockFs = mock(FileSystem.class);

    when(mockFs.listStatus(new Path(histFolder))).thenThrow(new IOException("IO Excpt"));
    List<Path> actualRes = HdfsUtils.getMetadataFilePaths(mockFs, histFolder);

    Assert.assertEquals(new ArrayList<Path>(), actualRes);
    Assert.assertEquals(0, actualRes.size());
  }
}
