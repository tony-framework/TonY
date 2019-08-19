package hadoop;

import java.io.File;
import javax.inject.Singleton;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import play.Logger;

import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.HADOOP_CONF_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_SITE_CONFIGURATION_FILE;


/**
 * The class handles setting up HDFS configuration
 */
@Singleton
public class Configuration {
  private static final Logger.ALogger LOG = Logger.of(Configuration.class);

  private static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
  private static final String HDFS_SITE_CONF = "hdfs-site.xml";

  private static HdfsConfiguration hdfsConf;
  private static YarnConfiguration yarnConf;

  public Configuration() {
    hdfsConf = new HdfsConfiguration();
    yarnConf = new YarnConfiguration();

    String hadoopConfDir = System.getenv(HADOOP_CONF_DIR.key());
    if (hadoopConfDir != null) {
      Path coreSitePath = new Path(hadoopConfDir + File.separatorChar + CORE_SITE_CONF);

      hdfsConf.addResource(coreSitePath);
      hdfsConf.addResource(new Path(hadoopConfDir + File.separatorChar + HDFS_SITE_CONF));

      yarnConf.addResource(coreSitePath);
      yarnConf.addResource(new Path(hadoopConfDir + File.separatorChar + YARN_SITE_CONFIGURATION_FILE));
    }

    // return `kerberos` if on Kerberized cluster.
    // return `simple` if on unsecure cluster.
    LOG.info("Hadoop Auth Setting: " + hdfsConf.get("hadoop.security.authentication"));
  }

  public HdfsConfiguration getHdfsConf() {
    return hdfsConf;
  }

  public YarnConfiguration getYarnConf() {
    return yarnConf;
  }
}
