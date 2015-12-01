package com.nr8.analytics.r8port;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.junit.Test;

public class DateTimeDeserializerTest {

  private String TEST_JSON = "{\"recordId\":\"6c3e8f79-d9c5-981e-cccd-37ef9b0a69af\",\"timestamp\":\"2015-11-30T06:11:27.630Z\",\"timeSincePageload\":870.3800000000001,\"component\":\"nav:$stateChangeSuccess\",\"sid\":\"xnfAfxwPU6u9nuqfVbt693nTIXVJ7YHJ\",\"username\":\"rclayton\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36\",\"reportDate\":\"2015-11-30T06:11:27.397Z\",\"eventData\":{\"component\":\"nav:$stateChangeSuccess\",\"username\":\"rclayton\",\"type\":\"$stateChangeSuccess\",\"toState\":{\"url\":\"/nr8s\",\"templateUrl\":\"articles/views/list.html\",\"controller\":\"ListController\",\"resolve\":{},\"name\":\"articles_list\"},\"toParams\":{},\"fromState\":{\"name\":\"\",\"url\":\"^\",\"views\":null,\"abstract\":true},\"fromParams\":{}}}\n";

  private String TEST_JSON2 = "{\"recordId\":\"78d7e889-821c-d001-ca39-b3b77a4452a9\",\"timestamp\":\"2015-11-30T14:58:38.504Z\",\"timeSincePageload\":842750.275,\"component\":\"/unkn:9074e4a7-117f-7d7a-4b3c-de4da7c80518/unkn:ee798f64-3693-cc9d-e879-32488ae55a3b\",\"sid\":\"aCEG3RXu5TipSFS6XjvfOoVXzFYc1wPv\",\"username\":\"rclayton\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36\",\"reportDate\":\"2015-11-30T14:44:36.440Z\",\"eventData\":\"hello\"}\n";

  @Test
  public void test_parse_datetime_as_object_property(){

    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(DateTime.class, new DateTimeDeserializer());
    Gson gson = gsonBuilder.create();

    R8port r8port = gson.fromJson(TEST_JSON, R8port.class);

    System.out.println(r8port);
  }

}
