package controllers;

import com.typesafe.config.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.inject.Inject;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import play.Logger.ALogger;
import play.Logger;
import play.mvc.*;


public class HomeController extends Controller {
	private static final ALogger LOG = Logger.of(HomeController.class);
	private final Config config;

	private final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();
	private final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
	private final String HDFS_SITE_CONF = "hdfs-site.xml";

	@Inject
	public HomeController(Config config) {
		this.config = config;
	}

	private HdfsConfiguration setUpHdfsConf() {
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
				UserGroupInformation.loginUserFromKeytab(config.getString("keytab.user"), config.getString("keytab.location"));
			} catch (IOException e) {
				LOG.error("Failed to set up keytab", e);
			}
		}
	}

	private FileSystem initFs(HdfsConfiguration hdfsConf) {
		try {
			return FileSystem.get(hdfsConf);
		} catch (IOException e) {
			LOG.error("Failed to instantiate HDFS FileSystem object", e);
		}
		return null;
	}

	private String contentOf(FileSystem fs, String tonyConfigPath) throws IOException {
		StringBuilder fileContent = new StringBuilder();
		Path dstPath = new Path(tonyConfigPath);

		try (FSDataInputStream inStrm = fs.open(dstPath);
				BufferedReader bufReader = new BufferedReader(new InputStreamReader(inStrm))) {
			String line;
			while ((line = bufReader.readLine()) != null) {
				fileContent.append(line);
			}
		}
		return fileContent.toString();
	}

	public Result index() {
		String fileContent;
		HdfsConfiguration hdfsConf = setUpHdfsConf();
		setUpKeytab(hdfsConf);
		FileSystem myFs = initFs(hdfsConf);
		LOG.info("Successfully instantiated file system");

		try {
			LOG.debug("Fs schema: " + myFs.getScheme());
			fileContent = contentOf(myFs, config.getString("tony.configPath"));
		} catch (IOException e) {
			LOG.error("Failed when reading config file", e);
			return ok(views.html.index.render(e.toString()));
		}
		return ok(views.html.index.render(fileContent));
	}
}