package utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import play.Logger;


/**
 * The class handles all configuration-related operations
 */
public class ConfigUtils {
  private static final Logger.ALogger LOG = Logger.of(ConfigUtils.class);

  /**
   * Check to see if configuration was passed in.
   * @param conf Config object. This is the wrapper of all args passed in from cmd line.
   * @return value of config if declared, {@code defaultVal} otherwise.
   */
  public static String fetchConfigIfExists(Config conf, String key, String defaultVal) {
    String value = defaultVal;
    try {
      value = conf.getString(key);
    } catch (ConfigException.Missing e) {
      LOG.warn("Failed to fetch value for `" + key + "`. Using `" + defaultVal + "` instead.", e);
    }
    return value;
  }

  public static int fetchIntConfigIfExists(Config conf, String key, int defaultVal) {
    if (conf.hasPath(key)) {
      return conf.getInt(key);
    }
    return defaultVal;
  }

  private ConfigUtils() { }
}
