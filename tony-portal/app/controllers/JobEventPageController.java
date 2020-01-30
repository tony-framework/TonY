package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobEvent;
import com.linkedin.tony.models.JobLog;
import com.linkedin.tony.util.HdfsUtils;
import com.linkedin.tony.util.ParserUtils;
import com.linkedin.tony.events.Event;
import hadoop.Requirements;
import java.util.List;
import javax.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.mvc.Controller;
import play.mvc.Result;


public class JobEventPageController extends Controller {
  private FileSystem myFs;
  private Cache<String, List<JobEvent>> cache;
  private Cache<String, List<JobLog>> jobLogCache;
  private Path interm;
  private Path finished;


  @Inject
  public JobEventPageController(Requirements requirements, CacheWrapper cacheWrapper) {
    myFs = requirements.getFileSystem();
    cache = cacheWrapper.getEventCache();
    jobLogCache = cacheWrapper.getLogCache();
    interm = requirements.getIntermediateDir();
    finished = requirements.getFinishedDir();
  }

  public Result index(String jobId) {
    List<JobEvent> listOfEvents;
    if (myFs == null) {
      return internalServerError("Failed to initialize file system in " + this.getClass());
    }

    // Check cache
    listOfEvents = cache.getIfPresent(jobId);
    if (listOfEvents != null) {
      return ok(views.html.event.render(listOfEvents));
    }

    // Check finished dir
    Path jobFolder = HdfsUtils.getJobDirPath(myFs, finished, jobId);
    if (jobFolder != null) {
      List<Event> events = ParserUtils.parseEvents(myFs, jobFolder);
      listOfEvents = ParserUtils.mapEventToJobEvent(events, jobId);
      cache.put(jobId, listOfEvents);
      // Since file is already parsed , its better to populate job log cache
      //jobLogCache.put(jobId, ParserUtils.mapEventToJobLog(events));
      return ok(views.html.event.render(listOfEvents));
    }

    // Check intermediate dir
    jobFolder = HdfsUtils.getJobDirPath(myFs, interm, jobId);
    if (jobFolder != null) {
      return internalServerError("Cannot display events because job is still running");
    }

    return internalServerError("Failed to fetch events");
  }


}
