package controllers;

import cache.CacheWrapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobEvent;
import com.linkedin.tony.util.HdfsUtils;
import hadoop.Configuration;
import hadoop.Requirements;
import java.util.List;
import javax.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static com.linkedin.tony.util.HdfsUtils.*;
import static com.linkedin.tony.util.ParserUtils.*;


public class JobEventPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobEventPageController.class);
  private HdfsConfiguration conf;
  private FileSystem myFs;
  private Cache<String, List<JobEvent>> cache;
  private Path finished;

  @Inject
  public JobEventPageController() {
    conf = Configuration.getHdfsConf();
    myFs = HdfsUtils.getFileSystem(conf);
    cache = CacheWrapper.getEventCache();
    finished = Requirements.getFinishedDir();
  }

  public Result index(String jobId) {
    List<JobEvent> listOfEvents;
    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    listOfEvents = cache.getIfPresent(jobId);
    if (listOfEvents != null) {
      return ok(views.html.event.render(listOfEvents));
    }
    List<Path> jobFolder = getJobFolders(myFs, finished, jobId);
    // There should only be 1 folder since jobId is unique
    Preconditions.checkArgument(jobFolder.size() == 1);
    listOfEvents = mapEventToJobEvent(parseEvents(myFs, jobFolder.get(0)));
    if (listOfEvents.size() == 0) {
      LOG.error("Failed to fetch list of events");
      return internalServerError("Failed to fetch events");
    }
    cache.put(jobId, listOfEvents);
    return ok(views.html.event.render(listOfEvents));
  }
}
