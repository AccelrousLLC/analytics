package com.nr8.analytics.r8port.config.models;


import com.nr8.analytics.r8port.config.ConfigReference;

import java.io.Serializable;

public class SessionEndedReportsSparkStreamConfig implements Serializable {

  private ConfigReference<KafkaConfig> kafkaUserSessionEndStream;

  private ConfigReference<CassandraConfig> cassandraR8portStorage;
  private ConfigReference<CassandraConfig> cassandraSessionStats;

  private int batchingWindow = 2;
  private String clusterMode = "local[4]";
  private String sparkAppName = "SessionAnalyzer";

  public ConfigReference<KafkaConfig> getKafkaUserSessionEndStream() {
    return kafkaUserSessionEndStream;
  }

  public void setKafkaUserSessionEndStream(ConfigReference<KafkaConfig> kafkaUserSessionEndStream) {
    this.kafkaUserSessionEndStream = kafkaUserSessionEndStream;
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

  public ConfigReference<CassandraConfig> getCassandraR8portStorage() {
    return cassandraR8portStorage;
  }

  public void setCassandraR8portStorage(ConfigReference<CassandraConfig> cassandraR8portStorage) {
    this.cassandraR8portStorage = cassandraR8portStorage;
  }

  public ConfigReference<CassandraConfig> getCassandraSessionStats() {
    return cassandraSessionStats;
  }

  public void setCassandraSessionStats(ConfigReference<CassandraConfig> cassandraSessionStats) {
    this.cassandraSessionStats= cassandraSessionStats;
  }
}
