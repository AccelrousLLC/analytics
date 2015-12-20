package com.nr8.analytics.r8port.config.models;


import com.nr8.analytics.r8port.config.ConfigReference;

import java.io.Serializable;

public class SessionEndedReportsSparkStreamConfig implements Serializable {

  private ConfigReference<KafkaConfig> kafkaUserSessionEndStream;

  private ConfigReference<DynamoConfig> dynamoR8portStorage;
  private ConfigReference<DynamoConfig> dynamoSessionStats;

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

  public ConfigReference<DynamoConfig> getDynamoR8portStorage() {
    return dynamoR8portStorage;
  }

  public void setDynamoR8portStorage(ConfigReference<DynamoConfig> dynamoR8portStorage) {
    this.dynamoR8portStorage = dynamoR8portStorage;
  }

  public ConfigReference<DynamoConfig> getDynamoSessionStats() {
    return dynamoSessionStats;
  }

  public void setDynamoSessionStats(ConfigReference<DynamoConfig> dynamoSessionStats) {
    this.dynamoSessionStats= dynamoSessionStats;
  }
}
