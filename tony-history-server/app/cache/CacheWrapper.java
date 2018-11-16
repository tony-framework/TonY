package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import javax.inject.Singleton;
import models.JobConfig;
import models.JobMetadata;
import org.apache.hadoop.fs.Path;
import play.Logger;

@Singleton
public class CacheWrapper {
  private static final Logger.ALogger LOG = Logger.of(CacheWrapper.class);
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
