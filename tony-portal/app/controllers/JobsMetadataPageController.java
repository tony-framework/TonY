package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.linkedin.tony.models.JobMetadata;
import java.util.Comparator;
import java.util.stream.Collectors;
import javax.inject.Inject;
import play.mvc.Controller;
import play.mvc.Result;


public class JobsMetadataPageController extends Controller {
  private Cache<String, JobMetadata> cache;

  @Inject
  public JobsMetadataPageController(CacheWrapper cacheWrapper) {
    cache = cacheWrapper.getMetadataCache();
  }


  public Result index() {

    return ok(views.html.metadata.render(cache.asMap().values()
        .stream()
        .sorted(Comparator.comparingLong(JobMetadata::getCompleted).reversed())
        .collect(Collectors.toList())));
  }
}



