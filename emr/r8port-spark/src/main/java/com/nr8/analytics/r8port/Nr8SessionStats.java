package com.nr8.analytics.r8port;

import org.joda.time.DateTime;

public class Nr8SessionStats {
  public final String sessionID;
  public final DateTime startTime;
  public final DateTime endTime;
  public final long duration;
  public final String username;

  public Nr8SessionStats(String sessionID, DateTime startTime, DateTime endTime) {
    this(sessionID, startTime, endTime, null);
  }

  public Nr8SessionStats(String sessionID, DateTime startTime, DateTime endTime, String username) {
    this.sessionID = sessionID;
    this.startTime = startTime;
    this.endTime = endTime;
    this.duration = (endTime.getMillis() - startTime.getMillis()) / 1000;
    this.username = username;

    if (duration < 0) {
      throw new IllegalArgumentException("Start time cannot be greater than end time");
    }
  }
}
