package com.nr8.analytics.r8port;

import com.google.gson.JsonObject;
import org.joda.time.DateTime;

import java.io.Serializable;

public class R8port implements Serializable {

  private String recordId;
  private DateTime timestamp;
  private double timeSincePageload;
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

  public double getTimeSincePageload() {
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

  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  public void setTimestamp(DateTime timestamp) {
    this.timestamp = timestamp;
  }

  public void setTimeSincePageload(double timeSincePageload) {
    this.timeSincePageload = timeSincePageload;
  }

  public void setComponent(String component) {
    this.component = component;
  }

  public void setSid(String sid) {
    this.sid = sid;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }

  public void setReportDate(DateTime reportDate) {
    this.reportDate = reportDate;
  }

  public void setEventData(JsonObject eventData) {
    this.eventData = eventData;
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
