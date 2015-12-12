package com.nr8.analytics.r8port.config.impl;

import com.google.common.base.Optional;
import com.nr8.analytics.r8port.config.models.KafkaConfig;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConsulConfigLoaderTest {

  @Test
  public void testGetConfig() throws Exception {
    String uri = "consul://consul.nr8.com:8500";

    ConsulConfigLoader loader = new ConsulConfigLoader(uri);

    Optional<KafkaConfig> optionalKC = loader.getConfig("production", "kafka", KafkaConfig.class);

    KafkaConfig kafkaConfig = optionalKC.get();
  }

  @Test
  public void testGetResponseBody() throws Exception {

  }

  @Test
  public void testParseURI() throws Exception {

    String uri = "consul://consul.nr8.com:8500?secure=true&password=meanioio&username=nr8admin";

    Map<String, String> params = ConsulConfigLoader.parseURI(uri);

    assertTrue(params.containsKey("url"));
    assertTrue(params.containsKey("secure"));
    assertTrue(params.containsKey("username"));
    assertTrue(params.containsKey("password"));

    assertEquals("consul.nr8.com:8500", params.get("url"));
    assertEquals("true", params.get("secure"));
    assertEquals("nr8admin", params.get("username"));
    assertEquals("meanioio", params.get("password"));
  }
}