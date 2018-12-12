package controllers;

import cache.CacheWrapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.linkedin.tony.TonyConfigurationKeys;
import com.typesafe.config.Config;
import hadoop.Configuration;
import java.util.List;
import javax.inject.Inject;
import models.JobEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;
import utils.HdfsUtils;

import static utils.HdfsUtils.*;
import static utils.ParserUtils.*;


public class JobEventPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobEventPageController.class);
  private final Config config;

  @Inject
  public JobEventPageController(Config config) {
    this.config = config;
  }

  public Result index(String jobId) {
    HdfsConfiguration conf = Configuration.getHdfsConf();
    FileSystem myFs = HdfsUtils.getFileSystem(conf);
    Cache<String, List<JobEvent>> cache = CacheWrapper.getEventCache();

    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    List<JobEvent> listOfEvents;
    Path tonyHistoryFolder = new Path(config.getString(TonyConfigurationKeys.TONY_HISTORY_LOCATION));
    listOfEvents = cache.getIfPresent(jobId);
    if (listOfEvents != null) {
      return ok(views.html.event.render(listOfEvents));
    }
    List<Path> jobFolder = getJobFolders(myFs, tonyHistoryFolder, jobId);
    // There should only be 1 folder since jobId is unique
    Preconditions.checkArgument(jobFolder.size() == 1);
    listOfEvents = parseEvents(myFs, jobFolder.get(0));
    if (listOfEvents.size() == 0) {
      LOG.error("Failed to fetch list of events");
      return internalServerError("Failed to fetch events");
    }
    cache.put(jobId, listOfEvents);
    return ok(views.html.event.render(listOfEvents));
  }
}
