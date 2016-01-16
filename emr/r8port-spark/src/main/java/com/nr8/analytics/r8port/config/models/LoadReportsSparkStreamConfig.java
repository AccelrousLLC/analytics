package com.nr8.analytics.r8port.config.models;


import com.nr8.analytics.r8port.config.ConfigReference;

import java.io.Serializable;

public class LoadReportsSparkStreamConfig implements Serializable {

  private ConfigReference<KafkaConfig> kafkaUserActivityStream;
  private ConfigReference<KafkaConfig> kafkaUserSessionEndStream;

  private ConfigReference<CassandraConfig> cassandra;

  private String r8portKafkaTopic = "user_activity";
  private int batchingWindow = 2;
  private String clusterMode = "local[4]";
  private String sparkAppName = "R8portLoader";

  public ConfigReference<KafkaConfig> getKafkaUserActivityStream() {
    return kafkaUserActivityStream;
  }

  public void setKafkaUserActivityStream(ConfigReference<KafkaConfig> kafkaUserActivityStream) {
    this.kafkaUserActivityStream = kafkaUserActivityStream;
  }

  public ConfigReference<KafkaConfig> getKafkaUserSessionEndStream() {
    return kafkaUserSessionEndStream;
  }

  public void setKafkaUserSessionEndStream(ConfigReference<KafkaConfig> kafkaUserSessionEndStream) {
    this.kafkaUserSessionEndStream = kafkaUserSessionEndStream;
  }

  public String getR8portKafkaTopic() {
    return r8portKafkaTopic;
  }

  public void setR8portKafkaTopic(String r8portKafkaTopic) {
    this.r8portKafkaTopic = r8portKafkaTopic;
  }

  public int getBatchingWindow() {
    return batchingWindow;
  }

  public void setBatchingWindow(int batchingWindow) {
    this.batchingWindow = batchingWindow;
  }

  public String getClusterMode() {
    return clusterMode;
  }

  public void setClusterMode(String clusterMode) {
    this.clusterMode = clusterMode;
  }

  public String getSparkAppName() {
    return sparkAppName;
  }

  public void setSparkAppName(String sparkAppName) {
    this.sparkAppName = sparkAppName;
  }

  public ConfigReference<CassandraConfig> getCassandra() {
    return cassandra;
  }

  public void setCassandra(ConfigReference<CassandraConfig> cassandra) {
    this.cassandra = cassandra;
  }
}
