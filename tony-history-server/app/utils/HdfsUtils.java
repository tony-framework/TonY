package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.io.IOUtils;
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
   * List all metadata file paths in {@code histFolder}.
   * @param fs FileSystem object.
   * @param histFolder full path of the history folder.
   * @return A list of metadata Path objects.
   */
  public static List<Path> getMetadataFilePaths(FileSystem fs, String histFolder) {
    List<Path> paths = new ArrayList<>();
    FileStatus[] lsHist;
    try {
      lsHist = fs.listStatus(new Path(histFolder));
    } catch (IOException e) {
      LOG.error("Failed to read history folder", e);
      return paths;
    }

    for (FileStatus nodes : lsHist) {
      if (nodes.isDirectory()) {
        paths.add(new Path(nodes.getPath().toString() + "/metadata.json"));
      }
    }
    return paths;
  }

  public static FileSystem getFileSystem(HdfsConfiguration hdfsConf) {
    try {
      return FileSystem.get(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to instantiate HDFS FileSystem object", e);
    }
    return null;
  }

  private HdfsUtils() {
  }
}
