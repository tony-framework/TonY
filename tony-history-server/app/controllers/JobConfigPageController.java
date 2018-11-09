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

import security.HadoopSecurity;


public class JobConfigPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobConfigPageController.class);
  private final Config config;
  private HadoopSecurity hadoopSec;

  @Inject
  public JobConfigPageController(Config config) {
    this.config = config;
    hadoopSec = HadoopSecurity.getInstance(config);
  }

  public Result index(String jobId) {
    List<JobConfig> listOfConfigs = new ArrayList<>();
    FileSystem myFs;

    try {
      myFs = hadoopSec.getInitializedFs();
    } catch (Exception e) {
      return internalServerError("Failed to initialize file system", e.toString());
    }
    String tonyHistoryFolder = config.getString("tony.historyFolder");
    Path xmlPath = new Path(tonyHistoryFolder + jobId + "/config.xml");

    listOfConfigs = parseConfig(myFs, xmlPath);
    if (listOfConfigs.size() == 0) {
      LOG.error("Failed to fetch list of configs");
      return internalServerError("Failed to fetch configuration");
    }

    return ok(views.html.config.render(listOfConfigs));
  }
}
