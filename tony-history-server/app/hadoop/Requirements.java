package hadoop;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.util.Utils;
import com.typesafe.config.Config;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
  private static Path histFolder;
  private static Path interm;
  private static Path finished;

  private void createDirIfNotExists(FileSystem myFs, Path dir, FsPermission perm) {
    String errorMsg;
    try {
      if (!myFs.exists(dir)) {
        errorMsg = dir + " doesn't exist";
        LOG.error(errorMsg);
        LOG.info("Creating " + dir);
        Utils.createDir(myFs, dir, perm);
      }
    } catch (IOException e) {
      errorMsg = "Failed to check " + dir + " existence";
      LOG.error(errorMsg, e);
    }
  }

  @Inject
  public Requirements(Config appConf) {
    HdfsConfiguration conf = Configuration.getHdfsConf();
    FileSystem myFs = HdfsUtils.getFileSystem(conf);
    if (myFs == null) {
      LOG.error("Failed to initialize file system");
      return;
    }

    histFolder = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_LOCATION,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_LOCATION));
    interm = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_INTERMEDIATE));
    finished = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_FINISHED,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_FINISHED));

    createDirIfNotExists(myFs, histFolder, Constants.perm777);
    createDirIfNotExists(myFs, interm, Constants.perm777);
    createDirIfNotExists(myFs, finished, Constants.perm770);
  }

  public static Path getFinishedDir() {
    return finished;
  }

  public static Path getIntermDir() {
    return interm;
  }
}
