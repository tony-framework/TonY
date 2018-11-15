package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import javax.inject.Singleton;
import models.JobConfig;
import models.JobMetadata;
import play.Logger;

@Singleton
public class CacheWrapper {
  private static final Logger.ALogger LOG = Logger.of(CacheWrapper.class);
  private static Cache<String, JobMetadata> metadataCache;
  private static Cache<String, JobConfig> configCache;

  public CacheWrapper() {
    metadataCache = CacheBuilder.newBuilder().build();
    configCache = CacheBuilder.newBuilder().build();
  }

  public static Cache<String, JobMetadata> getMetadataCache() {
    return metadataCache;
  }

  public static Cache<String, JobConfig> getConfigCache() {
    return configCache;
  }
}
