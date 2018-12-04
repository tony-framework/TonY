package utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;


/**
 * The class handles all HDFS file operations
 */
public class HdfsUtils {
  private static final Logger.ALogger LOG = Logger.of(HdfsUtils.class);
  private static final FileStatus[] NO_FILES = {};

  /**
   * Delete all job directories in {@code jobDirs} array.
   * @param fs FileSystem object.
   * @param jobDirs An array of job directories.
   */
  public static void deleteMultiDir(FileSystem fs, FileStatus[] jobDirs) {
    for (FileStatus d : jobDirs) {
      try {
        fs.delete(d.getPath(), true);
      } catch (IOException e) {
        LOG.error("Failed to clean up " + d);
      }
    }
  }

  /**
   * Scan {@code dir} and return a list of files/directories.
   * @param fs FileSystem object.
   * @param dir Path of the directory.
   * @return an array of FileStatus objects representing files/directories in {@code dir}.
   * Note: result could be an empty array if there {@code dir} is empty.
   */
  public static FileStatus[] scanDir(FileSystem fs, Path dir) {
    String errorMsg;
    if (dir == null) {
      return NO_FILES;
    }
    try {
      return fs.listStatus(dir);
    } catch (IOException e) {
      errorMsg = "Failed to list files in " + dir;
      LOG.error(errorMsg);
    }
    return NO_FILES;
  }

  /**
   * Check to see if HDFS path exists.
   * @param fs FileSystem object.
   * @param filePath path of file to validate.
   * @return true if path is valid, false otherwise.
   */
  static boolean pathExists(FileSystem fs, Path filePath) {
    try {
      return fs.exists(filePath);
    } catch (IOException e) {
      LOG.error("Error when reading " + filePath.toString(), e);
      return false;
    }
  }

  /**
   * Return a string which contains the content of file on HDFS.
   * @param fs FileSystem object.
   * @param filePath path of file to read from.
   * @return the content of the file, or empty string if errors occur during read.
   */
  static String contentOfHdfsFile(FileSystem fs, Path filePath) {
    StringBuilder fileContent = new StringBuilder();
    try (FSDataInputStream inStrm = fs.open(filePath);
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(inStrm))) {
      String line;
      while ((line = bufReader.readLine()) != null) {
        fileContent.append(line);
      }
      return fileContent.toString();
    } catch (IOException e) {
      LOG.error("Couldn't read content of file from HDFS", e);
      return "";
    }
  }

  /**
   * Extract the job id portion from the file path string.
   * @param path file path string.
   * @return job id
   */
  public static String getJobId(String path) {
    if (path == null) {
      return "";
    }
    String[] folderLayers = path.split("/");
    return folderLayers[folderLayers.length - 1];
  }

  /**
   * Check to see if the path {@code p} is a job folder path from the given
   * folder name pattern {@code regex}
   * @param p Path object
   * @param regex regular expression string
   * @return true if path {@code p} is a job folder path, false otherwise.
   */
  @VisibleForTesting
  static boolean isJobFolder(Path p, String regex) {
    return p != null && getJobId(p.toString()).matches(regex);
  }

  /**
   * Find all job folders recursively under {@code curr} that
   * matches {@code regex} pattern and return a list of
   * corresponding Path objects.
   * @param fs FileSystem object.
   * @param curr folder location Path object.
   * @param regex regular expression string.
   * @return list of job Path objects in {@code curr} folder.
   */
  public static List<Path> getJobFolders(FileSystem fs, Path curr, String regex) {
    List<Path> intermediateFolders = new ArrayList<>();
    if (curr == null) {
      return Collections.emptyList();
    }
    if (isJobFolder(curr, regex)) {
      intermediateFolders.add(curr);
      return intermediateFolders;
    }
    try {
      intermediateFolders = Arrays.stream(fs.listStatus(curr))
          .filter(FileStatus::isDirectory)
          .map(fileStatus -> getJobFolders(fs, fileStatus.getPath(), regex))
          .flatMap(List::stream)
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.error("Failed to traverse down history folder", e);
    }
    return intermediateFolders;
  }

  public static FileSystem getFileSystem(HdfsConfiguration hdfsConf) {
    try {
      return FileSystem.get(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to instantiate HDFS FileSystem object", e);
    }
    return null;
  }

  private HdfsUtils() { }
}
