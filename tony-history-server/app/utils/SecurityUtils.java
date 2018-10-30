package utils;

import com.typesafe.config.Config;
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
public class SecurityUtils {
  private static final Logger.ALogger LOG = Logger.of(SecurityUtils.class);
  private static Config appConf;
  private static FileSystem fs;

  private static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();
  private static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
  private static final String HDFS_SITE_CONF = "hdfs-site.xml";

  private static SecurityUtils instance = null;

  public static SecurityUtils getInstance(Config conf) {
    if (instance == null) {
      instance = new SecurityUtils(conf);
    }

    return instance;
  }

  private SecurityUtils(Config appConf) {
    HdfsConfiguration hdfsConf = setUpHdfsConf();
    SecurityUtils.appConf = appConf;
    setUpKeytab(hdfsConf);
    fs = getFileSystem(hdfsConf);
  }

  private static HdfsConfiguration setUpHdfsConf() {
    HdfsConfiguration conf = new HdfsConfiguration();

    if (System.getenv("HADOOP_CONF_DIR") != null) {
      conf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      conf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    }

    // return `kerberos` if on Kerberized cluster.
    // return `simple` if develop locally.
    LOG.debug("Hadoop Auth Setting: " + conf.get("hadoop.security.authentication"));
    return conf;
  }

  private void setUpKeytab(HdfsConfiguration hdfsConf) {
    boolean isSecurityEnabled = hdfsConf.get("hadoop.security.authentication").equals("kerberos");
    if (isSecurityEnabled) {
      try {
        UserGroupInformation.setConfiguration(hdfsConf);
        UserGroupInformation.loginUserFromKeytab(appConf.getString("keytab.user"), appConf.getString("keytab.location"));
      } catch (IOException e) {
        LOG.error("Failed to set up keytab", e);
      }
    }
  }

  private static FileSystem getFileSystem(HdfsConfiguration hdfsConf) {
    try {
      return FileSystem.get(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to instantiate HDFS FileSystem object", e);
    }
    return null;
  }

  public static FileSystem getFs() {
    return fs;
  }
}
