package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobEvent;
import com.linkedin.tony.models.JobLog;
import javax.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.mvc.Controller;
import play.mvc.Result;
import hadoop.Requirements;
import java.util.List;
import com.linkedin.tony.util.HdfsUtils;
import com.linkedin.tony.util.ParserUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import com.linkedin.tony.models.JobMetadata;
import java.util.stream.Collectors;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.util.JobLogMetaData;
import com.linkedin.tony.util.Utils;


public class JobLogPageController extends Controller {
  private FileSystem myFs;
  private Cache<String, List<JobEvent>> jobEventCache;
  private Cache<String, List<JobLog>> jobLogCache;
  private Path interm;
  private Path finished;
  private YarnConfiguration yarnConf;
  private Cache<String, JobMetadata> metaDataCache;

  @Inject
  public JobLogPageController(Requirements requirements, CacheWrapper cacheWrapper) {
    myFs = requirements.getFileSystem();
    jobEventCache = cacheWrapper.getEventCache();
    jobLogCache = cacheWrapper.getLogCache();
    interm = requirements.getIntermediateDir();
    finished = requirements.getFinishedDir();
    yarnConf = cacheWrapper.getYarnConf();
    metaDataCache = cacheWrapper.getMetadataCache();
  }

  public Result index(String jobId) {
    List<JobLog> listOflogs;
    if (myFs == null) {
      return internalServerError("Failed to initialize file system in " + this.getClass());
    }

    // Check Log cache
    listOflogs = jobLogCache.getIfPresent(jobId);
    if (listOflogs != null) {
      return ok(views.html.log.render(listOflogs, Utils.linksToBeDisplayedOnPage(jobId)));
    }

    String userName = getUserNameFromMetaDataCache(jobId);

    //Check job event cache , if the file already parsed . Use that information
    //to create joblogs
    List<JobEvent> jobEvents = jobEventCache.getIfPresent(jobId);
    if (jobEvents != null) {
      listOflogs = parseJobEventsToJobLogs(userName, jobEvents);
      jobLogCache.put(jobId, listOflogs);
      return ok(views.html.log.render(listOflogs, Utils.linksToBeDisplayedOnPage(jobId)));
    }

    //If the job log doesn't exist in cache and also not there in job event cache
    // Parse the file
    //Check finished dir , if the file is not parsed even once .
    Path jobFolder = HdfsUtils.getJobDirPath(myFs, finished, jobId);
    if (jobFolder != null) {
      List<Event> events = ParserUtils.parseEvents(myFs, jobFolder);
      listOflogs = ParserUtils.mapEventToJobLog(events, new JobLogMetaData(yarnConf, userName));
      jobLogCache.put(jobId, listOflogs);
      //Since file is already parsed , its better populate event cache
      jobEventCache.put(jobId, ParserUtils.mapEventToJobEvent(events));
      return ok(views.html.log.render(listOflogs, Utils.linksToBeDisplayedOnPage(jobId)));
    }

    // Check intermediate dir
    jobFolder = HdfsUtils.getJobDirPath(myFs, interm, jobId);
    if (jobFolder != null) {
      return internalServerError("Cannot display events because job is still running");
    }

    return internalServerError("Failed to fetch events");
  }

  /**
   *
   * @param jobID jobId provided by the user
   * @return user who launch the application
   */
  private String getUserNameFromMetaDataCache(String jobID) {
    String userName = null;
    if (metaDataCache != null && metaDataCache.getIfPresent(jobID) != null) {
      userName = metaDataCache.getIfPresent(jobID).getUser();
    }
    return userName;
  }

  /**
   *
   * @param userName userName of the one who launced application
   * @param jobEvents list of job events
   * @return list of job logs
   */
  private List<JobLog> parseJobEventsToJobLogs(String userName, List<JobEvent> jobEvents) {
    List<Event> events = jobEvents.stream()
        .map(jobEvent -> new Event(jobEvent.getType(), jobEvent.getEvent(), jobEvent.getTimestamp().getTime()))
        .collect(Collectors.toList());
    List<JobLog> listOflogs = ParserUtils.mapEventToJobLog(events, new JobLogMetaData(yarnConf, userName));
    return listOflogs;
  }
}
