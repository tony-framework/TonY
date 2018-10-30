package controllers;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import models.JobConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static utils.HdfsUtils.*;
import static utils.ParserUtils.*;
import static utils.SecurityUtils.*;


public class JobsConfigPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobsConfigPageController.class);
  private final Config config;

  @Inject
  public JobsConfigPageController(Config config) {
    this.config = config;
    getInstance(config);
  }

  public Result index(String jobId) {
    List<JobConfig> listOfConfigs = new ArrayList<>();
    FileSystem myFs = getFs();
    String tonyHistoryFolder = config.getString("tony.historyFolder");
    List<Path> paths = getFilePathsFromOneJob(myFs, tonyHistoryFolder, jobId, "xml");

    if (paths.size() > 0) {
      try {
        // hardcode first item since we only have one xml file (config file)
        listOfConfigs = parseConfig(myFs, paths.get(0));
      } catch (Exception e) {
        LOG.error("Failed to fetch list of configs", e);
      }
    }

    return ok(views.html.config.render(listOfConfigs));
  }
}
