package com.nr8.analytics.r8port.config.models;

import com.google.common.base.Optional;
import com.nr8.analytics.r8port.config.ConfigLoader;
import com.nr8.analytics.r8port.config.ConfigLoaderFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class LoadReportsSparkStreamConfigTest {

  @Test
  public void test_nested_retrieval_of_properties(){

    ConfigLoader loader = ConfigLoaderFactory.getLoader("consul://consul.nr8.com:8500").get();

    Optional<LoadReportsSparkStreamConfig> config =
        loader.getConfig("production", "spark.load-r8ports", LoadReportsSparkStreamConfig.class);

    assertTrue(config.isPresent());

    Optional<KafkaConfig> kafka = config.get().getKafka().get(KafkaConfig.class);

    assertTrue(kafka.isPresent());

    assertEquals("kafka-production.nr8.com:9092", kafka.get().getBrokers());
  }


}