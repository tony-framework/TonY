package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestHdfsUtils {
  //  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
//  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
//  private final PrintStream originalOut = System.out;
//  private final PrintStream originalErr = System.err;

  @Mock
  private BufferedReader bufReader;

  @InjectMocks
  private HdfsUtils _hdfsUtils;

//  @Before
//  public void setUpStreams() {
//    System.setOut(new PrintStream(outContent));
//    System.setErr(new PrintStream(errContent));
//  }

  @Test
  public void testIsPathValid_pathValid() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path validPath = new Path("/valid/path");
    when(mockFs.exists(validPath)).thenReturn(true);

    assertTrue(HdfsUtils.isPathValid(mockFs, validPath));
    verify(mockFs).exists(validPath);
  }

  @Test
  public void testIsPathValid_pathNotValid() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    Path invalidPath = new Path("/invalid/path");
    when(mockFs.exists(invalidPath)).thenReturn(false);

    assertFalse(HdfsUtils.isPathValid(mockFs, invalidPath));
    verify(mockFs).exists(invalidPath);
  }

//  @Test
//  public void testContentOfHdfsFile_hdfsFileWithContent() throws IOException {
//    MockitoAnnotations.initMocks(this);
//    FileSystem mockFs = mock(FileSystem.class);
//    Path filePath = new Path("/file/path/with/content");
//
//    FSDataInputStream inStrm = mock(FSDataInputStream.class);
//    when(mockFs.open(filePath)).thenReturn(inStrm);
//    when(bufReader.readLine()).thenReturn("someContent");
//
//
////    when(new BufferedReader(any(InputStreamReader.class)).readLine()).thenReturn("someContent");
//
//    assertEquals("someContent", HdfsUtils.contentOfHdfsFile(mockFs, filePath));
//    verify(mockFs).open(filePath);
//  }

  @Test
  public void testGetValidPaths_emptyList() {
    FileStatus fileStatus1 = mock(FileStatus.class);
    FileStatus fileStatus2 = mock(FileStatus.class);
    FileStatus fileStatus3 = mock(FileStatus.class);
    FileStatus fileStatus4 = mock(FileStatus.class);
    FileStatus[] fileStatuses = new FileStatus[]{fileStatus1, fileStatus2, fileStatus3, fileStatus4};
    Path mockPth = mock(Path.class);

    when(fileStatus1.getPath()).thenReturn(mockPth);
    when(fileStatus2.getPath()).thenReturn(mockPth);
    when(fileStatus3.getPath()).thenReturn(mockPth);
    when(fileStatus4.getPath()).thenReturn(mockPth);
    when(mockPth.toString()).thenReturn("");

    assertEquals(new ArrayList<>(), HdfsUtils.getValidPaths(fileStatuses, fileStatus -> fileStatus.getPath().toString().startsWith("doesn't exist")));
  }

  @Test
  public void testGetValidPaths_normalResult() {
    FileStatus fileStatus1 = mock(FileStatus.class);
    FileStatus fileStatus2 = mock(FileStatus.class);
    FileStatus fileStatus3 = mock(FileStatus.class);
    FileStatus fileStatus4 = mock(FileStatus.class);
    FileStatus[] fileStatuses = new FileStatus[]{fileStatus1, fileStatus2, fileStatus3, fileStatus4};
    Path validPath = mock(Path.class);
    Path invalidPath = mock(Path.class);

    when(fileStatus1.getPath()).thenReturn(validPath);
    when(fileStatus2.getPath()).thenReturn(validPath);
    when(fileStatus3.getPath()).thenReturn(invalidPath);
    when(fileStatus4.getPath()).thenReturn(validPath);
    when(validPath.toString()).thenReturn("name");
    when(invalidPath.toString()).thenReturn("notname");

    assertEquals(3, HdfsUtils.getValidPaths(fileStatuses, fileStatus -> fileStatus.getPath().toString().startsWith("name")).size());
    assertEquals(fileStatus1.getPath(), HdfsUtils.getValidPaths(fileStatuses, fileStatus -> fileStatus.getPath().toString().startsWith("name")).get(0));
    assertEquals(fileStatus2.getPath(), HdfsUtils.getValidPaths(fileStatuses, fileStatus -> fileStatus.getPath().toString().startsWith("name")).get(1));
    assertEquals(fileStatus4.getPath(), HdfsUtils.getValidPaths(fileStatuses, fileStatus -> fileStatus.getPath().toString().startsWith("name")).get(2));
  }

  @Test
  public void testGetFilePathsFromAllJobs_emptyList() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    FileStatus fileStatus1 = mock(FileStatus.class);
    FileStatus fileStatus2 = mock(FileStatus.class);
    FileStatus fileStatus3 = mock(FileStatus.class);
    FileStatus fileStatus4 = mock(FileStatus.class);
    FileStatus jobDir1 = mock(FileStatus.class);
    FileStatus jobDir2 = mock(FileStatus.class);
    FileStatus jobDir3 = mock(FileStatus.class);
    FileStatus jobDir4 = mock(FileStatus.class);
    FileStatus[] fileStatuses = new FileStatus[]{fileStatus1, fileStatus2, fileStatus3, fileStatus4};
    FileStatus[] jobDirs = new FileStatus[]{jobDir1, jobDir2, jobDir3, jobDir4};
    Path mockPth = mock(Path.class);
    Path mockDirPth = mock(Path.class);
    String fileType = "xml";
    String histFolder = "/test/hist/folder";
    String jobDirPath = "/path/to/job";
    String jobFilePath = "/path/to/job/job.json";

    when(fileStatus1.getPath()).thenReturn(mockPth);
    when(fileStatus2.getPath()).thenReturn(mockPth);
    when(fileStatus3.getPath()).thenReturn(mockPth);
    when(fileStatus4.getPath()).thenReturn(mockPth);
    when(jobDir1.isDirectory()).thenReturn(true);
    when(jobDir2.isDirectory()).thenReturn(true);
    when(jobDir3.isDirectory()).thenReturn(true);
    when(jobDir4.isDirectory()).thenReturn(true);
    when(jobDir1.getPath()).thenReturn(mockDirPth);
    when(jobDir2.getPath()).thenReturn(mockDirPth);
    when(jobDir3.getPath()).thenReturn(mockDirPth);
    when(jobDir4.getPath()).thenReturn(mockDirPth);
    when(mockDirPth.toString()).thenReturn(jobDirPath);
    when(mockPth.toString()).thenReturn(jobFilePath);
    when(mockFs.listStatus(new Path(jobDirPath))).thenReturn(fileStatuses);
    when(mockFs.listStatus(new Path(histFolder))).thenReturn(jobDirs);

    assertEquals(new ArrayList<>(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType));
  }

  @Test
  public void testGetFilePathsFromAllJobs_normalResult() throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    FileStatus fileStatus1 = mock(FileStatus.class);
    FileStatus fileStatus2 = mock(FileStatus.class);
    FileStatus fileStatus3 = mock(FileStatus.class);
    FileStatus fileStatus4 = mock(FileStatus.class);
    FileStatus jobDir1 = mock(FileStatus.class);
    FileStatus jobDir2 = mock(FileStatus.class);
    FileStatus jobDir3 = mock(FileStatus.class);
    FileStatus jobDir4 = mock(FileStatus.class);
    FileStatus[] fileStatuses = new FileStatus[]{fileStatus1, fileStatus2, fileStatus3, fileStatus4};
    FileStatus[] jobDirs = new FileStatus[]{jobDir1, jobDir2, jobDir3, jobDir4};
    Path validPath = mock(Path.class);
    Path invalidPath = mock(Path.class);
    Path mockDirPth = mock(Path.class);
    String fileType = "xml";
    String histFolder = "/test/hist/folder";
    String jobDirPath = "/path/to/job";
    String validJobFilePath = "/path/to/job/job.xml";
    String invalidJobFilePath = "/path/to/job/job.json";

    when(fileStatus1.getPath()).thenReturn(validPath);
    when(fileStatus2.getPath()).thenReturn(validPath);
    when(fileStatus3.getPath()).thenReturn(invalidPath); // this should be filtered out
    when(fileStatus4.getPath()).thenReturn(validPath);
    when(jobDir1.isDirectory()).thenReturn(true);
    when(jobDir2.isDirectory()).thenReturn(false); // this should be filtered out
    when(jobDir3.isDirectory()).thenReturn(true);
    when(jobDir4.isDirectory()).thenReturn(true);
    when(jobDir1.getPath()).thenReturn(mockDirPth);
    when(jobDir2.getPath()).thenReturn(mockDirPth);
    when(jobDir3.getPath()).thenReturn(mockDirPth);
    when(jobDir4.getPath()).thenReturn(mockDirPth);
    when(mockDirPth.toString()).thenReturn(jobDirPath);
    when(validPath.toString()).thenReturn(validJobFilePath);
    when(invalidPath.toString()).thenReturn(invalidJobFilePath);
    when(mockFs.listStatus(new Path(jobDirPath))).thenReturn(fileStatuses);
    when(mockFs.listStatus(new Path(histFolder))).thenReturn(jobDirs);

    /**
     * Reason:
     * - Max number of valid paths: 16 (4 job dirs x 4 files)
     * - jobDir2 is not a directory -> Paths = 16 - 4 = 12
     * - fileStatus3 is not a valid path (ended with json instead of xml)
     * in 3 of the directories (jobDir1, jobDir2, jobDir3) -> Paths = 12 - 3 = 9
     */
    assertEquals(9, HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).size());
    assertEquals(fileStatus1.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(0));
    assertEquals(fileStatus2.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(1));
    assertEquals(fileStatus4.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(2));
    assertEquals(fileStatus1.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(3));
    assertEquals(fileStatus2.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(4));
    assertEquals(fileStatus4.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(5));
    assertEquals(fileStatus1.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(6));
    assertEquals(fileStatus2.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(7));
    assertEquals(fileStatus4.getPath(), HdfsUtils.getFilePathsFromAllJobs(mockFs, histFolder, fileType).get(8));
  }
}
