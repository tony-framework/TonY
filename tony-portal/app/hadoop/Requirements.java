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
import org.apache.hadoop.security.UserGroupInformation;
import play.Logger;
import utils.ConfigUtils;


/**
 * The class checks all the directory requirements (intermediate, finished)
 */
@Singleton
public class Requirements {
  private static final Logger.ALogger LOG = Logger.of(Requirements.class);

  private String keytabUser;
  private String keytabLocation;

  private FileSystem histFs;
  private Path histFolder;
  private Path interm;
  private Path finished;

  public FileSystem getFileSystem() {
    return histFs;
  }

  private void createDirIfNotExists(FileSystem myFs, Path dir, FsPermission perm) {
    String errorMsg;
    try {
      if (!myFs.exists(dir)) {
        errorMsg = dir + " doesn't exist";
        LOG.warn(errorMsg);
        LOG.info("Creating " + dir);
        Utils.createDirIfNotExists(myFs, dir, perm);
      }
    } catch (IOException e) {
      errorMsg = "Failed to check " + dir + " existence";
      LOG.error(errorMsg, e);
    }
  }

  private void initFs(HdfsConfiguration conf) {
    try {
      histFs = histFolder.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Encountered exception while getting filesystem for " + histFolder, e);
    }
  }

  private void setupKeytab(Config appConf) {
    keytabUser = ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_KEYTAB_USER,
        TonyConfigurationKeys.DEFAULT_TONY_KEYTAB_USER);
    keytabLocation = ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_KEYTAB_LOCATION,
        TonyConfigurationKeys.DEFAULT_TONY_KEYTAB_LOCATION);

    try {
      UserGroupInformation.loginUserFromKeytab(keytabUser, keytabLocation);
    } catch (IOException e) {
      LOG.error("Failed to login with user " + keytabUser + " from keytab file " + keytabLocation, e);
    }
  }

  private void setupSecurity(Config appConf, HdfsConfiguration hdfsConf) {
    boolean isSecurityEnabled = hdfsConf.get("hadoop.security.authentication").equals("kerberos");
    if (isSecurityEnabled) {
      UserGroupInformation.setConfiguration(hdfsConf);
      setupKeytab(appConf);
    }
  }

  @Inject
  public Requirements(Config appConf, Configuration configuration) {
    HdfsConfiguration hdfsConf = configuration.getHdfsConf();

    // Must be done before initializing the filesystem so FS is initialized with Kerberos
    setupSecurity(appConf, hdfsConf);

    histFolder = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_LOCATION,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_LOCATION));
    initFs(hdfsConf);
    if (histFs == null) {
      LOG.error("Failed to initialize history file system");
      return;
    }

    interm = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_INTERMEDIATE,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_INTERMEDIATE));
    finished = new Path(ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_FINISHED,
        TonyConfigurationKeys.DEFAULT_TONY_HISTORY_FINISHED));

    createDirIfNotExists(histFs, histFolder, Constants.PERM777);
    createDirIfNotExists(histFs, interm, Constants.PERM777);
    createDirIfNotExists(histFs, finished, Constants.PERM770);
  }

  public Path getFinishedDir() {
    return finished;
  }

  public Path getIntermediateDir() {
    return interm;
  }
}
