import cache.CacheWrapper;
import com.google.inject.AbstractModule;
import hadoop.Configuration;
import hadoop.Requirements;


/**
 * Play automatically registers the module {@code Module} located in the root package.
 * See {@href https://www.playframework.com/documentation/2.7.x/JavaDependencyInjection#Programmatic-bindings}
 * for more details.
 */
public class Module extends AbstractModule {
  @Override
  public void configure() {
    bind(Configuration.class).asEagerSingleton();
    bind(Requirements.class).asEagerSingleton();
    bind(CacheWrapper.class).asEagerSingleton();
    // add module for moving files from intermediate to finished
  }
}
