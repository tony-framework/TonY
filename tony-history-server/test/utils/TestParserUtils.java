package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import models.JobConfig;
import models.JobMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
    Path jobFolder = new Path("./test/resources/typicalHistFolder/job1");
    String jobRegex = "application\\d+";
    JobMetadata expected =
        new JobMetadata("application123", "/jobs/application123", "/config/application123", 1, 1, "SUCCEEDED", "user1");
    JobMetadata actual = ParserUtils.parseMetadata(fs, jobFolder, jobRegex);

    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getJobLink(), actual.getJobLink());
    assertEquals(expected.getConfigLink(), actual.getConfigLink());
    assertEquals(expected.getStartedDate(), actual.getStartedDate());
    assertEquals(expected.getCompletedDate(), actual.getCompletedDate());
    assertEquals(expected.getStatus(), actual.getStatus());
    assertEquals(expected.getUser(), actual.getUser());
  }

  @Test
  public void testParseMetadata_fail_IOException() throws IOException {
    Path jobFolder = new Path("./test/resources/typicalHistFolder/job1");
    String jobRegex = "application\\d+";
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listStatus(jobFolder)).thenThrow(new IOException("IO Excpt"));

    JobMetadata result = ParserUtils.parseMetadata(mockFs, jobFolder, jobRegex);
    assertNull(result);
  }

  @Test
  public void testParseConfig_success() {
    Path jobFolder = new Path("./test/resources/typicalHistFolder/job1");
    List<JobConfig> expected = new ArrayList<>();
    JobConfig expectedConfig = new JobConfig();
    expectedConfig.setName("name");
    expectedConfig.setValue("value");
    expectedConfig.setFinal(true);
    expectedConfig.setSource("source");

    expected.add(expectedConfig);
    List<JobConfig> actual = ParserUtils.parseConfig(fs, jobFolder);

    assertEquals(expected.size(), actual.size());
    assertEquals(expected.get(0).getName(), actual.get(0).getName());
    assertEquals(expected.get(0).getValue(), actual.get(0).getValue());
    assertEquals(expected.get(0).isFinal(), actual.get(0).isFinal());
    assertEquals(expected.get(0).getSource(), actual.get(0).getSource());
  }

  @Test
  public void testParseConfig_fail_IOException() throws IOException {
    Path jobFolder = new Path("./test/resources/typicalHistFolder/job1");
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.listStatus(jobFolder)).thenThrow(new IOException("IO Excpt"));

    List<JobConfig> loc = ParserUtils.parseConfig(mockFs, jobFolder);
    assertEquals(0, loc.size());
  }
}
