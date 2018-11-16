package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.typesafe.config.Config;
import hadoop.Configuration;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import models.JobConfig;
import models.JobMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static utils.ParserUtils.*;

import hadoop.Security;
import utils.HdfsUtils;


public class JobConfigPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobConfigPageController.class);
  private final Config config;

  @Inject
  public JobConfigPageController(Config config) {
    this.config = config;
  }

  public Result index(String jobId) {
    HdfsConfiguration conf = Configuration.getHdfsConf();
    FileSystem myFs = HdfsUtils.getFileSystem(conf);
    Cache<String, List<JobConfig>> cache = CacheWrapper.getConfigCache();

    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    List<JobConfig> listOfConfigs;
    String tonyHistoryFolder = config.getString("tony.historyFolder");
    Path xmlPath = new Path(tonyHistoryFolder + jobId + "/config.xml");
    if (cache.asMap().containsKey(jobId)) {
      listOfConfigs = cache.getIfPresent(jobId);
      return ok(views.html.config.render(listOfConfigs));
    }
    listOfConfigs = parseConfig(myFs, xmlPath);
    cache.put(jobId, listOfConfigs);
    if (listOfConfigs.size() == 0) {
      LOG.error("Failed to fetch list of configs");
      return internalServerError("Failed to fetch configuration");
    }

    return ok(views.html.config.render(listOfConfigs));
  }
}
