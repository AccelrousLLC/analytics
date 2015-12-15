package com.nr8.analytics.r8port.config.models;


import java.io.Serializable;

public class KafkaBrokerConfig implements Serializable {

  private String brokers;
  private String zookeepers;

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
}
