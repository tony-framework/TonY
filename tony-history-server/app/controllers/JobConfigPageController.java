package controllers;

import cache.CacheWrapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.util.HdfsUtils;
import hadoop.Configuration;
import hadoop.Requirements;
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
  private Path finished;

  public JobConfigPageController() {
    conf = Configuration.getHdfsConf();
    myFs = HdfsUtils.getFileSystem(conf);
    cache = CacheWrapper.getConfigCache();
    finished = Requirements.getFinishedDir();
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
    List<Path> jobFolder = getJobFolders(myFs, finished, jobId);
    // There should only be 1 folder since jobId is unique
    Preconditions.checkArgument(jobFolder.size() == 1);
    listOfConfigs = parseConfig(myFs, jobFolder.get(0));
    if (listOfConfigs.size() == 0) {
      LOG.error("Failed to fetch list of configs");
      return internalServerError("Failed to fetch configuration");
    }
    cache.put(jobId, listOfConfigs);
    return ok(views.html.config.render(listOfConfigs));
  }
}
