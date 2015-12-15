package com.nr8.analytics.r8port.services.kafka;

import com.nr8.analytics.r8port.config.models.KafkaBrokerConfig;
import com.nr8.analytics.r8port.config.models.KafkaConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaProducerService implements Closeable {
  private Producer<String, String> producer;
  private String topic;

  public KafkaProducerService(KafkaConfig config) {
    Properties props = new Properties();
    String brokers = config.getBroker().load(KafkaBrokerConfig.class).get().getBrokers();
    props.put("metadata.broker.list", brokers);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    props.put("zookeeper.connect", "localhost:2181");

    ProducerConfig producerConfig = new ProducerConfig(props);
    this.producer = new Producer<>(producerConfig);
    this.topic = config.getTopicName();
  }

  public KeyedMessage<String, String> getMessage(String key, String message) {
    return new KeyedMessage<>(this.topic, key, message);
  }

  public void send(String key, String message) {
    producer.send(getMessage(key, message));
  }

  public void send(KeyedMessage<String, String> msg) {
    producer.send(msg);
  }

  public void send(List<KeyedMessage<String, String>> msgs) {
    producer.send(msgs);
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }
}
