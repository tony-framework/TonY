package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import javax.inject.Singleton;
import models.JobConfig;
import models.JobMetadata;

@Singleton
public class CacheWrapper {
  private static Cache<String, JobMetadata> metadataCache;
  private static Cache<String, List<JobConfig>> configCache;

  public CacheWrapper() {
    metadataCache = CacheBuilder.newBuilder().build();
    configCache = CacheBuilder.newBuilder().build();
  }

  public static Cache<String, JobMetadata> getMetadataCache() {
    return metadataCache;
  }

  public static Cache<String, List<JobConfig>> getConfigCache() {
    return configCache;
  }
}
