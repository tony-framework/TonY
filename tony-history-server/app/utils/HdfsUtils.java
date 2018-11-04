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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.Logger;


/**
 * The class handles all HDFS file operations
 */
public class HdfsUtils {
  private static final Logger.ALogger LOG = Logger.of(HdfsUtils.class);

  static boolean isPathValid(FileSystem fs, Path filePath) {
    try {
      return fs.exists(filePath);
    } catch (IOException e) {
      LOG.error(filePath.toString() + " doesn't exist!");
      return false;
    }
  }

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

  static List<Path> getValidPaths(FileStatus[] lsJobDir, Predicate<FileStatus> fn) {
    return Arrays.stream(lsJobDir)
        .filter(fn)
        .map(FileStatus::getPath)
        .collect(Collectors.toList());
  }

  public static List<Path> getFilePathsFromAllJobs(FileSystem fs, String histFolder, String fileType) {
    List<Path> paths = new ArrayList<>();
    FileStatus[] lsHist;
    try {
      lsHist = fs.listStatus(new Path(histFolder));
    } catch (FileNotFoundException e) {
      LOG.error("Failed to locate history folder", e);
      return paths;
    } catch (IOException e) {
      LOG.error("Failed to scan history folder", e);
      return paths;
    }

    LOG.debug("lsHist size: " + lsHist.length);
    paths = Arrays.stream(lsHist).filter(FileStatus::isDirectory).map((item) -> {
      try {
        return getValidPaths(fs.listStatus(new Path(item.getPath().toString())),
            fileStatus -> fileStatus.getPath().toString().endsWith(fileType));
      } catch (FileNotFoundException e) {
        LOG.error("Failed to locate history folder", e);
        return new ArrayList<Path>();
      } catch (IOException e) {
        LOG.error("Failed to scan history folder", e);
        return new ArrayList<Path>();
      }
    }).flatMap(List::stream).collect(Collectors.toList());
    return paths;
  }

  public static List<Path> getFilePathsFromOneJob(FileSystem fs, String histFolder, String jobId, String fileType) {
    List<Path> paths = new ArrayList<>();
    StringBuilder jobDirSb = new StringBuilder();
    jobDirSb.append(histFolder);
    jobDirSb.append(jobId);
    jobDirSb.append("/");
    FileStatus[] lsJobDir;

    try {
      lsJobDir = fs.listStatus(new Path(jobDirSb.toString()));
    } catch (FileNotFoundException e) {
      LOG.error("Failed to locate history folder", e);
      return paths;
    } catch (IOException e) {
      LOG.error("Failed to scan history folder", e);
      return paths;
    }

    return getValidPaths(lsJobDir, fileStatus -> fileStatus.getPath().toString().endsWith(fileType));
  }

  private HdfsUtils() {}
}
