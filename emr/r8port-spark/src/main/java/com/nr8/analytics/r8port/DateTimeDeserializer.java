package com.nr8.analytics.r8port;

import com.google.gson.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.reflect.Type;

public class DateTimeDeserializer implements JsonDeserializer<DateTime>, JsonSerializer<DateTime> {

  DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
  DateTimeFormatter fmt = ISODateTimeFormat.dateTime();

  @Override
  public DateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {

    String dateTimeString = json.getAsString();

    return parser.parseDateTime(dateTimeString);
  }

  @Override
  public JsonElement serialize(DateTime src, Type typeOfSrc, JsonSerializationContext context) {

    String isoDatetime = fmt.print(src);

    return new JsonPrimitive(isoDatetime);
  }
}
