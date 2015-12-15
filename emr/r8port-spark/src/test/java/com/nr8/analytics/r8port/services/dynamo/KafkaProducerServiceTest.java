package com.nr8.analytics.r8port.services.dynamo;

import com.nr8.analytics.r8port.config.ConfigReference;
import com.nr8.analytics.r8port.config.models.KafkaBrokerConfig;
import com.nr8.analytics.r8port.config.models.KafkaConfig;
import com.nr8.analytics.r8port.services.kafka.KafkaProducerService;
import kafka.producer.KeyedMessage;
import org.junit.Test;
import static org.junit.Assert.*;

public class KafkaProducerServiceTest {
  private static KafkaProducerService getKafkaService() {
    KafkaConfig config = new KafkaConfig();
    config.setTopicName("session_ended");

    KafkaBrokerConfig brokerConfig = new KafkaBrokerConfig();

    brokerConfig.setBrokers("localhost:9092");
    brokerConfig.setZookeepers("localhost:2181");

    config.setBroker(new ConfigReference<>(brokerConfig));

    return new KafkaProducerService(config);
  }

  @Test
  public void testGetMessage() throws Exception {
    String key = "this is the key";
    String message = "this is the message";
    KafkaProducerService kafkaProducerService = getKafkaService();
    KeyedMessage<String, String> msg = kafkaProducerService.getMessage(key, message);

    assertNotNull(msg);
    assertEquals(key, msg.key());
    assertEquals(message, msg.message());
  }

  @Test
  public void testSend() throws Exception {
    String key = "this is another key";
    String message = "this is another message";
    KafkaProducerService kafkaProducerService = getKafkaService();
    kafkaProducerService.send(key, message);
  }

  @Test
  public void testClose() throws Exception {
    KafkaProducerService kafkaProducerService = getKafkaService();
    kafkaProducerService.close();
  }
}
