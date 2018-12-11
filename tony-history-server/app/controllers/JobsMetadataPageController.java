package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.linkedin.tony.Constants;
import com.linkedin.tony.util.Utils;
import hadoop.Configuration;
import hadoop.Requirements;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.JobMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;
import utils.HdfsUtils;

import static utils.HdfsUtils.*;
import static utils.ParserUtils.*;


public class JobsMetadataPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobsMetadataPageController.class);
  private static final String JOB_FOLDER_REGEX = "^application_\\d+_\\d+$";
  private HdfsConfiguration conf;
  private FileSystem myFs;
  private Cache<String, JobMetadata> cache;
  private Path interm;
  private Path finished;

  public JobsMetadataPageController() {
    conf = Configuration.getHdfsConf();
    myFs = HdfsUtils.getFileSystem(conf);
    cache = CacheWrapper.getMetadataCache();
    interm = Requirements.getIntermDir();
    finished = Requirements.getFinishedDir();
  }

  private void moveIntermToFinished(FileSystem fs, HdfsConfiguration conf, Map<String, Date> jobsAccessTime,
      Map<String, Path> jobFolders) {
    jobsAccessTime.forEach((id, date) -> {
      StringBuilder path = new StringBuilder(finished.toString());
      LocalDate ldate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      String[] directories = {Integer.toString(ldate.getYear()), Integer.toString(ldate.getMonthValue()),
          Integer.toString(ldate.getDayOfMonth())};
      for (String dir : directories) {
        path.append("/").append(dir);
        Utils.createDir(fs, new Path(path.toString()), Constants.perm770);
      }

      Path source = jobFolders.get(id);
      Path dest = new Path(path.toString());
      try {
        FileUtil.copy(fs, source, fs, dest, true, conf);
      } catch (IOException e) {
        LOG.error("Failed to move files from intermediate to finished", e);
      }
    });
  }

  private void storeJobData(Map<String, Date> jobsAccessTime, Map<String, Path> jobsFiles,
      FileStatus[] jobDirs) {
    for (FileStatus dir : jobDirs) {
      Path jobFolderPath = dir.getPath();
      String jid = HdfsUtils.getJobId(jobFolderPath.toString());
      jobsFiles.putIfAbsent(jid, jobFolderPath);
      jobsAccessTime.putIfAbsent(jid, new Date(dir.getAccessTime()));
    }
  }

  public Result index() {
    List<JobMetadata> listOfMetadata = new ArrayList<>();
    JobMetadata tmpMetadata;
    String jobId;

    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    FileStatus[] jobDirs = HdfsUtils.scanDir(myFs, interm);
    if (jobDirs.length > 0) {
      Map<String, Date> jobsAccessTime = new HashMap<>();
      Map<String, Path> jobsFiles = new HashMap<>();
      storeJobData(jobsAccessTime, jobsFiles, jobDirs);
      moveIntermToFinished(myFs, conf, jobsAccessTime, jobsFiles);
    }

    for (Path f : getJobFolders(myFs, finished, JOB_FOLDER_REGEX)) {
      jobId = getJobId(f.toString());
      tmpMetadata = cache.getIfPresent(jobId);
      if (tmpMetadata == null) {
        try {
          tmpMetadata = parseMetadata(myFs, f, JOB_FOLDER_REGEX);
          cache.put(jobId, tmpMetadata);
        } catch (Exception e) {
          LOG.error("Couldn't parse " + f, e);
          continue;
        }
      }
      listOfMetadata.add(tmpMetadata);
    }
    return ok(views.html.metadata.render(listOfMetadata));
  }
}