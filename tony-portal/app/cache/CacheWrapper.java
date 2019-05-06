package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.models.JobEvent;
import com.linkedin.tony.models.JobMetadata;
import com.linkedin.tony.util.HdfsUtils;
import com.linkedin.tony.util.ParserUtils;
import com.typesafe.config.Config;
import hadoop.Configuration;
import hadoop.Requirements;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import play.Logger;
import utils.ConfigUtils;



@Singleton
public class CacheWrapper {
  private static final Logger.ALogger LOG = Logger.of(CacheWrapper.class);
  private static final String JOB_FOLDER_REGEX = "^application_\\d+_\\d+$";

  private final YarnConfiguration yarnConf;
  private final FileSystem myFs;
  private final Path finishedDir;
  private final Path intermediateDir;

  /**
   * metadataCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: JobMetadata object containing all the metadata of the jobs (user, status, etc.)
   */
  private Cache<String, JobMetadata> metadataCache;

  /**
   * configCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: List of JobConfig objects. Each JobConfig object
   * represents a {@code property} (name-val-source-final) in config.xml
   */
  private Cache<String, List<JobConfig>> configCache;

  /**
   * eventCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: List of JobEvent objects. Each JobEvent object
   * represents an Event in job's jhist
   */
  private Cache<String, List<JobEvent>> eventCache;

  @Inject
  public CacheWrapper(Config appConf, Configuration conf, Requirements reqs) {
    int maxCacheSz = Integer.parseInt(
        ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_PORTAL_CACHE_MAX_ENTRIES,
            TonyConfigurationKeys.DEFAULT_TONY_PORTAL_CACHE_MAX_ENTRIES));
    metadataCache = CacheBuilder.newBuilder().maximumSize(maxCacheSz).build();
    configCache = CacheBuilder.newBuilder().maximumSize(maxCacheSz).build();
    eventCache = CacheBuilder.newBuilder().maximumSize(maxCacheSz).build();

    yarnConf = conf.getYarnConf();
    myFs = reqs.getFileSystem();
    finishedDir = reqs.getFinishedDir();
    intermediateDir = reqs.getIntermediateDir();

    initializeCachesAsync();
  }

  public Cache<String, JobMetadata> getMetadataCache() {
    return metadataCache;
  }

  public Cache<String, List<JobConfig>> getConfigCache() {
    return configCache;
  }

  public Cache<String, List<JobEvent>> getEventCache() {
    return eventCache;
  }

  public void updateCaches(Path jobDir) {
    String jobId = HdfsUtils.getLastComponent(jobDir.toString());
    JobMetadata metadata = ParserUtils.parseMetadata(myFs, yarnConf, jobDir, JOB_FOLDER_REGEX);
    List<JobConfig> configs = ParserUtils.parseConfig(myFs, jobDir);
    List<JobEvent> events = ParserUtils.mapEventToJobEvent(ParserUtils.parseEvents(myFs, jobDir));
    if (metadata != null) {
      metadataCache.put(jobId, metadata);
    }
    configCache.put(jobId, configs);
    eventCache.put(jobId, events);
  }

  private void initializeCachesAsync() {
    new Thread(() -> {
      LOG.info("Starting background initialization of caches.");
      List<Path> listOfJobDirs = HdfsUtils.getJobDirs(myFs, finishedDir, JOB_FOLDER_REGEX);
      listOfJobDirs.addAll(HdfsUtils.getJobDirs(myFs, intermediateDir, JOB_FOLDER_REGEX));
      listOfJobDirs.forEach(this::updateCaches);
      LOG.info("Done with background initialization of caches.");
    }).start();
  }
}
