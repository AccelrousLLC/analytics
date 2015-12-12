package com.nr8.analytics.r8port.config;

import com.google.common.base.Optional;
import com.nr8.analytics.r8port.config.impl.ConsulConfigLoader;
import com.nr8.analytics.r8port.config.impl.FileConfigLoader;

/**
 * Really simple factory to determine the correct configuration loader
 * based on a URI string.
 */
public class ConfigLoaderFactory {

  /**
   * Get the configuration.
   * @param uri URI string.
   * @return ConfigLoader or nothing.
   */
  public static Optional<ConfigLoader> getLoader(String uri){
    ConfigLoader loader = null;

    if (uri.startsWith(FileConfigLoader.URI_PREFIX))   loader = new FileConfigLoader(uri);
    if (uri.startsWith(ConsulConfigLoader.URI_PREFIX)) loader = new ConsulConfigLoader(uri);

    return Optional.fromNullable(loader);
  }

}
