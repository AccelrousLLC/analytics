package com.nr8.analytics.r8port;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;


public class R8portDeserializerTest {

  private String TEST_JSON = "{\"recordId\":\"6c3e8f79-d9c5-981e-cccd-37ef9b0a69af\",\"timestamp\":\"2015-11-30T06:11:27.630Z\",\"timeSincePageload\":870.3800000000001,\"component\":\"nav:$stateChangeSuccess\",\"sid\":\"xnfAfxwPU6u9nuqfVbt693nTIXVJ7YHJ\",\"username\":\"rclayton\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36\",\"reportDate\":\"2015-11-30T06:11:27.397Z\",\"eventData\":{\"component\":\"nav:$stateChangeSuccess\",\"username\":\"rclayton\",\"type\":\"$stateChangeSuccess\",\"toState\":{\"url\":\"/nr8s\",\"templateUrl\":\"articles/views/list.html\",\"controller\":\"ListController\",\"resolve\":{},\"name\":\"articles_list\"},\"toParams\":{},\"fromState\":{\"name\":\"\",\"url\":\"^\",\"views\":null,\"abstract\":true},\"fromParams\":{}}}\n";

  @Test
  public void testDeserialize() throws Exception {

    R8port r8port = JsonUtils.deserialize(TEST_JSON, R8port.class);

    System.out.println(r8port);

    String r8portAsJson = JsonUtils.serialize(r8port);

    System.out.println(r8portAsJson);
  }
}