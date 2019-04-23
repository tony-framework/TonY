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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  private YarnConfiguration yarnConf;
  private FileSystem myFs;
  private Cache<String, JobMetadata> cache;
  private Path interm;
  private Path finished;

  @Inject
  public JobsMetadataPageController(Configuration configuration, Requirements requirements, CacheWrapper cacheWrapper) {
    yarnConf = configuration.getYarnConf();
    myFs = requirements.getFileSystem();
    cache = cacheWrapper.getMetadataCache();
    interm = requirements.getIntermDir();
    finished = requirements.getFinishedDir();
  }

  private boolean jobInProgress(FileSystem fs, Path jobDir) {
    try {
      // If there is a file ending in ".jhist" (NOT ".jhist.inprogress"), the job is no longer in progress.
      // Otherwise, it is considered in progress.
      return !Arrays.stream(fs.listStatus(jobDir))
          .anyMatch(fileStatus -> fileStatus.getPath().toString().endsWith(Constants.HISTFILE_SUFFIX));
    } catch (IOException e) {
      LOG.error("Encountered exception reading " + jobDir, e);
      return false;
    }
  }

  private void moveIntermToFinished(FileSystem fs, Map<String, Date> jobsModTime,
      Map<String, Path> jobFolders) {
    jobsModTime.forEach((id, date) -> {
      Path source = jobFolders.get(id);
      if (jobInProgress(fs, source)) {
        return;
      }

      StringBuilder path = new StringBuilder(finished.toString());
      LocalDate ldate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      String[] directories = {
          Integer.toString(ldate.getYear()),
          String.format("%02d", ldate.getMonthValue()),
          String.format("%02d", ldate.getDayOfMonth())
      };
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

  private void storeJobData(Map<String, Date> jobsModTime, Map<String, Path> jobsFiles,
      FileStatus[] jobDirs) {
    for (FileStatus dir : jobDirs) {
      Path jobFolderPath = dir.getPath();
      String jid = HdfsUtils.getLastComponent(jobFolderPath.toString());
      jobsFiles.putIfAbsent(jid, jobFolderPath);
      jobsModTime.putIfAbsent(jid, new Date(dir.getModificationTime()));
    }
  }

  public Result index() {
    List<JobMetadata> listOfMetadata = new ArrayList<>();
    JobMetadata tmpMetadata;
    String jobId;

    if (myFs == null) {
      return internalServerError("Failed to initialize file system in " + this.getClass());
    }

    FileStatus[] jobDirs = new FileStatus[0];
    try {
      jobDirs = myFs.listStatus(interm);
    } catch (IOException e) {
      LOG.error("Failed to list files in " + interm, e);
    }
    if (jobDirs.length > 0) {
      Map<String, Date> jobsModTime = new HashMap<>();
      Map<String, Path> jobsFiles = new HashMap<>();
      storeJobData(jobsModTime, jobsFiles, jobDirs);
      moveIntermToFinished(myFs, jobsModTime, jobsFiles);
    }

    List<Path> listOfJobDirs = new ArrayList<>(getJobFolders(myFs, finished, JOB_FOLDER_REGEX));
    listOfJobDirs.addAll(getJobFolders(myFs, interm, JOB_FOLDER_REGEX));

    for (Path jobDir : listOfJobDirs) {
      jobId = getLastComponent(jobDir.toString());
      tmpMetadata = cache.getIfPresent(jobId);
      if (tmpMetadata == null || tmpMetadata.getStatus().equals(Constants.RUNNING)) {
        tmpMetadata = parseMetadata(myFs, yarnConf, jobDir, JOB_FOLDER_REGEX);
        if (tmpMetadata == null) {
          continue;
        }
        cache.put(jobId, tmpMetadata);
      }
      listOfMetadata.add(tmpMetadata);
    }
    return ok(views.html.metadata.render(listOfMetadata));
  }
}