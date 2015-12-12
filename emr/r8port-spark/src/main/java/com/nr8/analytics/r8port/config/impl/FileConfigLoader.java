package com.nr8.analytics.r8port.config.impl;

import com.google.common.base.Optional;
import com.nr8.analytics.r8port.config.ConfigLoader;

/**
 * Loads configuration from the File System.
 */
public class FileConfigLoader implements ConfigLoader {

  public static final String URI_PREFIX = "file://";

  protected final String uri;

  public FileConfigLoader(String uri){
    this.uri = uri;
  }

  @Override
  public <T> Optional<T> getConfig(String environment, String key, Class<T> type) {
    return Optional.absent();
  }
}
