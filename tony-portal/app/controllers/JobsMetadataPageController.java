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

  public Result index() {
    List<JobMetadata> listOfMetadata = new ArrayList<>();
    JobMetadata tmpMetadata;
    String jobId;

    if (myFs == null) {
      return internalServerError("Failed to initialize file system in " + this.getClass());
    }

    List<Path> listOfJobDirs = getJobDirs(myFs, finished, JOB_FOLDER_REGEX);
    listOfJobDirs.addAll(getJobDirs(myFs, interm, JOB_FOLDER_REGEX));

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
