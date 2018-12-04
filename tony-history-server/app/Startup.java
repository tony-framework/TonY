import cache.CacheWrapper;
import com.google.inject.AbstractModule;
import hadoop.Configuration;
import hadoop.Requirements;
import hadoop.Security;


public class Startup extends AbstractModule {
  @Override
  public void configure() {
    bind(Configuration.class).asEagerSingleton();
    bind(Security.class).asEagerSingleton();
    bind(Requirements.class).asEagerSingleton();
    bind(CacheWrapper.class).asEagerSingleton();
  }
}
