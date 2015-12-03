package com.nr8.analytics.r8port;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;

public class JsonUtils {

  static Gson gson;

  static {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(R8port.class, new R8portDeserializer());
    gsonBuilder.registerTypeAdapter(DateTime.class, new DateTimeDeserializer());
    gson = gsonBuilder.create();
  }

  public static String serialize(Object obj){
    return gson.toJson(obj);
  }

  public static <T> T deserialize(String json, Class<T> clazz){
    return gson.fromJson(json, clazz);
  }

}
