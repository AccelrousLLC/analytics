package com.nr8.analytics.r8port.config.models;


import java.io.Serializable;

public class KafkaConfig implements Serializable {

  private String brokers;
  private String zookeepers;
  private String topicName;
  private int partitions = 1;
  private int replication = 1;

  public String getBrokers() {
    return brokers;
  }

  public void setBrokers(String brokers) {
    this.brokers = brokers;
  }

  public String getZookeepers() {
    return zookeepers;
  }

  public void setZookeepers(String zookeepers) {
    this.zookeepers = zookeepers;
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
