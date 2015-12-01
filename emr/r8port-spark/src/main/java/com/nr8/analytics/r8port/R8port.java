package com.nr8.analytics.r8port;

import com.google.gson.JsonObject;
import org.joda.time.DateTime;

import java.io.Serializable;

public class R8port implements Serializable {

  private String recordId;
  private DateTime timestamp;
  private float timeSincePageload;
  private String component;
  private String sid;
  private String username;
  private String userAgent;
  private DateTime reportDate;
  private JsonObject eventData;

  public String getRecordId() {
    return recordId;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  public float getTimeSincePageload() {
    return timeSincePageload;
  }

  public String getComponent() {
    return component;
  }

  public String getSid() {
    return sid;
  }

  public String getUsername() {
    return username;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public DateTime getReportDate() {
    return reportDate;
  }

  public JsonObject getEventData() {
    return eventData;
  }

  @Override
  public String toString() {
    return "R8port{" +
        "recordId='" + recordId + '\'' +
        ", timestamp=" + timestamp +
        ", timeSincePageload=" + timeSincePageload +
        ", component='" + component + '\'' +
        ", sid='" + sid + '\'' +
        ", username='" + username + '\'' +
        ", userAgent='" + userAgent + '\'' +
        ", reportDate=" + reportDate +
        ", eventData='" + eventData + '\'' +
        '}';
  }
}
