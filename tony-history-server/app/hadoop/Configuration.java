package hadoop;

import java.io.File;
import javax.inject.Singleton;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import play.Logger;


/**
 * The class handles setting up HDFS configuration
 */
@Singleton
public class Configuration {
  private static final Logger.ALogger LOG = Logger.of(Configuration.class);

  private static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();
  private static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
  private static final String HDFS_SITE_CONF = "hdfs-site.xml";

  private static HdfsConfiguration hdfsConf = new HdfsConfiguration();
  private static YarnConfiguration yarnConf = new YarnConfiguration();

  public Configuration() {
    if (System.getenv("HADOOP_CONF_DIR") != null) {
      hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    }

    // return `kerberos` if on Kerberized cluster.
    // return `simple` if develop locally.
    LOG.debug("Hadoop Auth Setting: " + hdfsConf.get("hadoop.security.authentication"));
  }

  public static HdfsConfiguration getHdfsConf() {
    return hdfsConf;
  }

  public static YarnConfiguration getYarnConf() {
    return yarnConf;
  }
}
