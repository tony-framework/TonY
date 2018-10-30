package controllers;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import models.JobMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;

import static utils.HdfsUtils.*;
import static utils.ParserUtils.*;
import static utils.SecurityUtils.*;


public class JobsMetadataPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobsMetadataPageController.class);
  private final Config config;

  @Inject
  public JobsMetadataPageController(Config config) {
    this.config = config;
    getInstance(config);
  }

  public Result index() {
    FileSystem myFs = getFs();
    LOG.info("Successfully instantiated file system");

    List<JobMetadata> listOfMetadata = new ArrayList<>();
    String tonyHistoryFolder = config.getString("tony.historyFolder");

    for (Path f : getFilePathsFromAllJobs(myFs, tonyHistoryFolder, "json")) {
      listOfMetadata.add(parseMetadata(myFs, f));
    }

    return ok(views.html.metadata.render(listOfMetadata));
  }
}