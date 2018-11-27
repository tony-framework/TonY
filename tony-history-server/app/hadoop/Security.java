package hadoop;

import com.typesafe.config.Config;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
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
    keytabUser = appConf.getString("tony.keytab.user");
    keytabLocation = appConf.getString("tony.keytab.location");
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
