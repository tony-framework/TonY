package controllers;

import cache.CacheWrapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobEvent;
import hadoop.Requirements;
import java.util.List;
import javax.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static com.linkedin.tony.util.HdfsUtils.*;
import static com.linkedin.tony.util.ParserUtils.*;


public class JobEventPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobEventPageController.class);
  private FileSystem myFs;
  private Cache<String, List<JobEvent>> cache;
  private Path interm;
  private Path finished;

  @Inject
  public JobEventPageController(Requirements requirements, CacheWrapper cacheWrapper) {
    myFs = requirements.getFileSystem();
    cache = cacheWrapper.getEventCache();
    interm = requirements.getIntermDir();
    finished = requirements.getFinishedDir();
  }

  public Result index(String jobId) {
    List<JobEvent> listOfEvents;
    if (myFs == null) {
      return internalServerError("Failed to initialize file system in " + this.getClass());
    }

    listOfEvents = cache.getIfPresent(jobId);
    if (listOfEvents != null) {
      return ok(views.html.event.render(listOfEvents));
    }

    // If intermediate directory exists for this job, it is still running.
    if (getJobDirPath(myFs, interm, jobId) != null) {
      return internalServerError("Cannot display events because job is still running");
    }

    Path jobFolder = getJobDirPath(myFs, finished, jobId);
    listOfEvents = mapEventToJobEvent(parseEvents(myFs, jobFolder));
    if (listOfEvents.isEmpty()) {
      LOG.error("Failed to fetch list of events");
      return internalServerError("Failed to fetch events");
    }
    cache.put(jobId, listOfEvents);
    return ok(views.html.event.render(listOfEvents));
  }
}
