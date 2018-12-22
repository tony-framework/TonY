package controllers;

import cache.CacheWrapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.util.HdfsUtils;
import hadoop.Configuration;
import hadoop.Requirements;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static com.linkedin.tony.util.HdfsUtils.*;
import static com.linkedin.tony.util.ParserUtils.*;


public class JobConfigPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobConfigPageController.class);
  private HdfsConfiguration conf;
  private FileSystem myFs;
  private Cache<String, List<JobConfig>> cache;
  private Path interm;
  private Path finished;

  public JobConfigPageController() {
    conf = Configuration.getHdfsConf();
    myFs = HdfsUtils.getFileSystem(conf);
    cache = CacheWrapper.getConfigCache();
    interm = Requirements.getIntermDir();
    finished = Requirements.getFinishedDir();
  }

  private List<JobConfig> getAndStoreConfigs(String jobId, List<Path> jobDirs) {
    if (jobDirs.isEmpty()) {
      return Collections.emptyList();
    }
    List<JobConfig> listOfConfigs = parseConfig(myFs, jobDirs.get(0));
    if (listOfConfigs.isEmpty()) {
      return Collections.emptyList();
    }
    cache.put(jobId, listOfConfigs);
    return listOfConfigs;
  }

  public Result index(String jobId) {
    List<JobConfig> listOfConfigs;
    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    listOfConfigs = cache.getIfPresent(jobId);
    if (listOfConfigs != null) {
      return ok(views.html.config.render(listOfConfigs));
    }

    listOfConfigs = getAndStoreConfigs(jobId, getJobFolders(myFs, interm, jobId));
    if (!listOfConfigs.isEmpty()) {
      return ok(views.html.config.render(listOfConfigs));
    }

    listOfConfigs = getAndStoreConfigs(jobId, getJobFolders(myFs, finished, jobId));
    if (!listOfConfigs.isEmpty()) {
      return ok(views.html.config.render(listOfConfigs));
    }

    return internalServerError("Failed to fetch configs");
  }
}
