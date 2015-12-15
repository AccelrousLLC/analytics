package com.nr8.analytics.r8port.config.models;


import com.nr8.analytics.r8port.config.ConfigReference;

import java.io.Serializable;

public class KafkaConfig implements Serializable {

  ConfigReference<KafkaBrokerConfig> broker;
  private String topicName;
  private int partitions = 1;
  private int replication = 1;

  public ConfigReference<KafkaBrokerConfig> getBroker() {
    return broker;
  }

  public void setBroker(ConfigReference<KafkaBrokerConfig> broker) {
    this.broker = broker;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public int getReplication() {
    return replication;
  }

  public void setReplication(int replication) {
    this.replication = replication;
  }
}
