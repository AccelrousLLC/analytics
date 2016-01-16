package com.nr8.analytics.r8port.config.models;

import java.io.Serializable;

public class CassandraConfig implements Serializable {
  private String table;
  private String endpoint = "";

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }
}
