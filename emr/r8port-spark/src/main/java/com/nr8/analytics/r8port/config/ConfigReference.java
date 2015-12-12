package com.nr8.analytics.r8port.config;


import com.google.common.base.Optional;

import java.io.Serializable;

public class ConfigReference<T> implements Serializable {

  private Optional<T> cachedInstance = null;
  private String uri;
  private String environment;
  private String key;

  public String getUri() {

    return this.uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public String getEnvironment() {

    return this.environment;
  }

  public void setEnvironment(String environment) {
    this.environment = environment;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Optional<T> load(Class<T> clazz){
    if (cachedInstance != null) return cachedInstance;
    try {
      ConfigLoader loader = ConfigLoaderFactory.getLoader(this.uri).get();

      cachedInstance = loader.getConfig(this.environment, this.key, clazz);

      return cachedInstance;
    }
    catch(Exception e){
      return Optional.absent();
    }
  }

  public ConfigReference<T> refresh(){
    this.cachedInstance = null;
    return this;
  }
}
