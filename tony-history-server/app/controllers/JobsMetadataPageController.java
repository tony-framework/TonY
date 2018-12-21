package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.linkedin.tony.Constants;
import com.linkedin.tony.models.JobMetadata;
import com.linkedin.tony.util.HdfsUtils;
import com.linkedin.tony.util.Utils;
import hadoop.Configuration;
import hadoop.Requirements;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static com.linkedin.tony.util.HdfsUtils.*;
import static com.linkedin.tony.util.ParserUtils.*;


public class JobsMetadataPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobsMetadataPageController.class);
  private static final String JOB_FOLDER_REGEX = "^application_\\d+_\\d+$";
  private HdfsConfiguration hdfsConf;
  private YarnConfiguration yarnConf;
  private FileSystem myFs;
  private Cache<String, JobMetadata> cache;
  private Path interm;
  private Path finished;

  public JobsMetadataPageController() {
    yarnConf = Configuration.getYarnConf();
    hdfsConf = Configuration.getHdfsConf();
    myFs = HdfsUtils.getFileSystem(hdfsConf);
    cache = CacheWrapper.getMetadataCache();
    interm = Requirements.getIntermDir();
    finished = Requirements.getFinishedDir();
  }

  private boolean jobInprogress(FileSystem fs, Path jobDir) {
    FileStatus[] jobFiles;
    try {
      jobFiles = fs.listStatus(jobDir);
      for (FileStatus file : jobFiles) {
        if (file.getPath().toString().endsWith(Constants.INPROGRESS)) {
          return true;
        }
      }
    } catch (IOException e) {
      LOG.error("Couldn't list files in " + jobDir, e);
    }
    return false;
  }

  private void moveIntermToFinished(FileSystem fs, Map<String, Date> jobsAccessTime,
      Map<String, Path> jobFolders) {
    jobsAccessTime.forEach((id, date) -> {
      Path source = jobFolders.get(id);
      if (jobInprogress(fs, source)) {
        return;
      }

      StringBuilder path = new StringBuilder(finished.toString());
      LocalDate ldate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      String[] directories = {Integer.toString(ldate.getYear()), Integer.toString(ldate.getMonthValue()),
          Integer.toString(ldate.getDayOfMonth())};
      for (String dir : directories) {
        path.append("/").append(dir);
        Utils.createDir(fs, new Path(path.toString()), Constants.PERM770);
      }

      Path dest = new Path(path.toString());
      try {
        fs.rename(source, dest);
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
      moveIntermToFinished(myFs, jobsAccessTime, jobsFiles);
    }

    List<Path> listOfJobDirs = new ArrayList<>(getJobFolders(myFs, finished, JOB_FOLDER_REGEX));
    listOfJobDirs.addAll(getJobFolders(myFs, interm, JOB_FOLDER_REGEX));

    for (Path jobDir : listOfJobDirs) {
      jobId = getJobId(jobDir.toString());
      tmpMetadata = cache.getIfPresent(jobId);
      if (tmpMetadata == null || tmpMetadata.getStatus().equals(Constants.RUNNING)) {
        try {
          tmpMetadata = parseMetadata(myFs, yarnConf, jobDir, JOB_FOLDER_REGEX);
          cache.put(jobId, tmpMetadata);
        } catch (Exception e) {
          LOG.error("Couldn't parse " + jobDir, e);
          continue;
        }
      }
      listOfMetadata.add(tmpMetadata);
    }
    return ok(views.html.metadata.render(listOfMetadata));
  }
}