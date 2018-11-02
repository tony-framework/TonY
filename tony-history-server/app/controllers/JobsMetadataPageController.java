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
import utils.SecurityUtils;

import static utils.HdfsUtils.*;
import static utils.ParserUtils.*;


public class JobsMetadataPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobsMetadataPageController.class);
  private final Config config;

  @Inject
  public JobsMetadataPageController(Config config) {
    this.config = config;
    SecurityUtils.getInstance(config);
  }

  public Result index() {
    FileSystem myFs;

    try {
      myFs = SecurityUtils.getInitializedFs();
    } catch (Exception e) {
      return internalServerError("Failed to initialize file system", e.toString());
    }

    List<JobMetadata> listOfMetadata = new ArrayList<>();
    String tonyHistoryFolder = config.getString("tony.historyFolder");
    JobMetadata tmpMetadata;

    for (Path f : getFilePathsFromAllJobs(myFs, tonyHistoryFolder, "json")) {
      tmpMetadata = parseMetadata(myFs, f);
      if (tmpMetadata == null) {
        LOG.error("Couldn't parse " + f.toString());
        continue;
      }
      listOfMetadata.add(tmpMetadata);
    }

    return ok(views.html.metadata.render(listOfMetadata));
  }
}