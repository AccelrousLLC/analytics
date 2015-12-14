package com.nr8.analytics.r8port.services.dynamo;

import com.nr8.analytics.r8port.config.models.KafkaConfig;
import com.nr8.analytics.r8port.services.kafka.KafkaProducerService;
import kafka.producer.KeyedMessage;
import org.junit.Test;

public class KafkaProducerServiceTest {
  private static KafkaProducerService getKafkaService() {
    KafkaConfig config = new KafkaConfig();
    config.setBrokers("localhost:9092");
    config.setZookeepers("localhost:2181");
    config.setTopicName("session_ended");

    return new KafkaProducerService(config);
  }

  @Test
  public void testGetMessage() throws Exception {
    String key = "this is the key";
    String message = "this is the message";
    KafkaProducerService kafkaProducerService = getKafkaService();
    KeyedMessage<String, String> msg = kafkaProducerService.getMessage(key, message);

    assert msg != null;
    assert msg.key().equals(key);
    assert msg.message().equals(message);
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
