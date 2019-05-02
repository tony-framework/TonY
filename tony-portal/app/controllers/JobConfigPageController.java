package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobConfig;
import hadoop.Requirements;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.mvc.Controller;
import play.mvc.Result;

import static com.linkedin.tony.util.HdfsUtils.getJobDirPath;
import static com.linkedin.tony.util.ParserUtils.parseConfig;


public class JobConfigPageController extends Controller {
  private FileSystem myFs;
  private Cache<String, List<JobConfig>> cache;
  private Path interm;
  private Path finished;

  @Inject
  public JobConfigPageController(Requirements requirements, CacheWrapper cacheWrapper) {
    myFs = requirements.getFileSystem();
    cache = cacheWrapper.getConfigCache();
    interm = requirements.getIntermediateDir();
    finished = requirements.getFinishedDir();
  }

  private List<JobConfig> getAndStoreConfigs(String jobId, Path jobDir) {
    if (jobDir == null) {
      return Collections.emptyList();
    }
    List<JobConfig> listOfConfigs = parseConfig(myFs, jobDir);
    if (listOfConfigs.isEmpty()) {
      return Collections.emptyList();
    }
    cache.put(jobId, listOfConfigs);
    return listOfConfigs;
  }

  public Result index(String jobId) {
    List<JobConfig> listOfConfigs;
    if (myFs == null) {
      return internalServerError("Failed to initialize file system in " + this.getClass());
    }

    // Check cache
    listOfConfigs = cache.getIfPresent(jobId);
    if (listOfConfigs != null) {
      return ok(views.html.config.render(listOfConfigs));
    }

    // Check finished dir
    listOfConfigs = getAndStoreConfigs(jobId, getJobDirPath(myFs, finished, jobId));
    if (!listOfConfigs.isEmpty()) {
      return ok(views.html.config.render(listOfConfigs));
    }

    // Check intermediate dir
    listOfConfigs = getAndStoreConfigs(jobId, getJobDirPath(myFs, interm, jobId));
    if (!listOfConfigs.isEmpty()) {
      return ok(views.html.config.render(listOfConfigs));
    }

    return internalServerError("Failed to fetch configs");
  }
}
