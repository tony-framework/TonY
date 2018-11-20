package controllers;

import com.typesafe.config.Config;
import hadoop.Configuration;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import models.JobMetadata;
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


public class JobsMetadataPageController extends Controller {
private static final ALogger LOG = Logger.of(JobsMetadataPageController.class);
  private final Config config;
  private static final String JOB_FOLDER_REGEX = "^application_\\d+_\\d+$";

  @Inject
  public JobsMetadataPageController(Config config) {
    this.config = config;
  }

  public Result index() {
    HdfsConfiguration conf = Configuration.getHdfsConf();
    FileSystem myFs = HdfsUtils.getFileSystem(conf);

    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    List<JobMetadata> listOfMetadata = new ArrayList<>();
    Path tonyHistoryFolder = new Path(config.getString("tony.historyFolder"));
    JobMetadata tmpMetadata;

    for (Path f : getJobFolders(myFs, tonyHistoryFolder, JOB_FOLDER_REGEX)) {
      tmpMetadata = parseMetadata(myFs, f, JOB_FOLDER_REGEX);
      if (tmpMetadata == null) {
        LOG.error("Couldn't parse " + f);
        continue;
      }
      listOfMetadata.add(tmpMetadata);
    }

    return ok(views.html.metadata.render(listOfMetadata));
  }
}