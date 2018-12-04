package hadoop;

import com.linkedin.tony.TonyConfigurationKeys;
import com.typesafe.config.Config;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import utils.ConfigUtils;
import utils.HdfsUtils;


/**
 * The class checks all the directory requirements (intermediate, finished)
 */
@Singleton
public class Requirements {
  private static final Logger.ALogger LOG = Logger.of(Requirements.class);
  private static Path interm;
  private static Path finished;

  @Inject
  public Requirements(Config appConf) {
    HdfsConfiguration conf = Configuration.getHdfsConf();
    FileSystem myFs = HdfsUtils.getFileSystem(conf);
    String errorMsg;
    interm = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_INTERMEDIATE));
    finished = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_FINISHED,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_FINISHED));

    try {
      if (!myFs.exists(interm)) {
        errorMsg = "Intermediate directory doesn't exist";
        LOG.error(errorMsg);
      }
    } catch (IOException e) {
      errorMsg = "Failed to check intermediate directory existence";
      LOG.error(errorMsg);
    }

    try {
      if (!myFs.exists(finished)) {
        errorMsg = "Finished directory doesn't exist";
        LOG.error(errorMsg);
      }
    } catch (IOException e) {
      errorMsg = "Failed to check finished directory existence";
      LOG.error(errorMsg);
    }
  }

  public static Path getFinishedDir() {
    return finished;
  }

  public static Path getIntermDir() {
    return interm;
  }
}
