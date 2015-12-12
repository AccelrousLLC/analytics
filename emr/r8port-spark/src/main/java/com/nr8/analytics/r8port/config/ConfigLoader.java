package com.nr8.analytics.r8port.config;

import com.google.common.base.Optional;


public interface ConfigLoader {

    <T> Optional<T> getConfig(String environment, String key, Class<T> clazz);
}
