package hadoop;

import com.typesafe.config.Config;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import play.Logger;


/**
 * The class handles authentication when cluster is security enabled
 */
@Singleton
public class Security {
  private static final Logger.ALogger LOG = Logger.of(Security.class);
  private static String keytabUser;
  private static String keytabLocation;

  @Inject
  public Security(Config appConf) {
    keytabUser = appConf.getString("keytab.user");
    keytabLocation = appConf.getString("keytab.location");
    setUpKeytab(Configuration.getHdfsConf());
  }

  private void setUpKeytab(HdfsConfiguration hdfsConf) {
    boolean isSecurityEnabled = hdfsConf.get("hadoop.security.authentication").equals("kerberos");
    if (isSecurityEnabled) {
      try {
        UserGroupInformation.setConfiguration(hdfsConf);
        UserGroupInformation.loginUserFromKeytab(keytabUser, keytabLocation);
      } catch (IOException e) {
        LOG.error("Failed to set up keytab", e);
      }
    }
  }
}
