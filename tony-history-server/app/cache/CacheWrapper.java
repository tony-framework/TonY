package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.models.JobConfig;
import com.linkedin.tony.models.JobEvent;
import com.linkedin.tony.models.JobMetadata;
import com.typesafe.config.Config;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import utils.ConfigUtils;


@Singleton
public class CacheWrapper {
  /**
   * metadataCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: JobMetadata object containing all the metadata of the jobs (user, status, etc.)
   */
  private static Cache<String, JobMetadata> metadataCache;

  /**
   * configCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: List of JobConfig objects. Each JobConfig object
   * represents a {@code property} (name-val-source-final) in config.xml
   */
  private static Cache<String, List<JobConfig>> configCache;

  /**
   * eventCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: List of JobEvent objects. Each JobEvent object
   * represents an Event in job's jhist
   */
  private static Cache<String, List<JobEvent>> eventCache;

  @Inject
  public CacheWrapper(Config appConf) {
    int maxCacheSz = Integer.parseInt(
        ConfigUtils.fetchConfigIfExists(appConf, TonyConfigurationKeys.TONY_HISTORY_CACHE_MAX_ENTRIES,
            TonyConfigurationKeys.DEFAULT_TONY_HISTORY_CACHE_MAX_ENTRIES));
    metadataCache = CacheBuilder.newBuilder().maximumSize(maxCacheSz).build();
    configCache = CacheBuilder.newBuilder().maximumSize(maxCacheSz).build();
    eventCache = CacheBuilder.newBuilder().maximumSize(maxCacheSz).build();
  }

  public static Cache<String, JobMetadata> getMetadataCache() {
    return metadataCache;
  }

  public static Cache<String, List<JobConfig>> getConfigCache() {
    return configCache;
  }

  public static Cache<String, List<JobEvent>> getEventCache() {
    return eventCache;
  }
}
