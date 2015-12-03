package com.nr8.analytics.r8port;

import com.google.gson.*;
import org.joda.time.DateTime;

import java.lang.reflect.Type;

public class R8portDeserializer implements JsonDeserializer<R8port> {

  @Override
  public R8port deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

    R8port r8port = new R8port();

    JsonObject root = json.getAsJsonObject();

    r8port.setRecordId(root.getAsJsonPrimitive("recordId").getAsString());

    DateTime timestamp = context.deserialize(root.getAsJsonPrimitive("timestamp"), DateTime.class);

    r8port.setTimestamp(timestamp);

    r8port.setTimeSincePageload(root.getAsJsonPrimitive("timeSincePageload").getAsDouble());

    r8port.setComponent(root.getAsJsonPrimitive("component").getAsString());

    r8port.setSid(root.getAsJsonPrimitive("sid").getAsString());

    r8port.setUsername(root.getAsJsonPrimitive("username").getAsString());

    r8port.setUserAgent(root.getAsJsonPrimitive("userAgent").getAsString());

    DateTime reportDate = context.deserialize(root.getAsJsonPrimitive("reportDate"), DateTime.class);

    r8port.setReportDate(reportDate);

    r8port.setEventData(root.getAsJsonObject("eventData"));

    return r8port;
  }
}
