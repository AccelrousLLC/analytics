package com.nr8.analytics.r8port.services.dynamo;

import com.clearspring.analytics.util.Lists;
import com.google.gson.JsonObject;
import com.nr8.analytics.r8port.R8port;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class DynamoR8portStorageServiceTest {

  @Test
  public void testPutRecords() throws Exception {

    DynamoR8portStorageService storageService = new DynamoR8portStorageService();

    R8port r8port = new R8port();

    r8port.setEventData(new JsonObject());
    r8port.setReportDate(new DateTime());
    r8port.setUserAgent("This is a test of the public broadcast system.");
    r8port.setUsername("rclayton");
    r8port.setComponent("spark-unit-test");
    r8port.setRecordId("96D0EF73-313E-4D0A-AD23-4FE6BE8A8C0D");
    r8port.setSid("lklaksdjflkasdashdfihqweoifhkjnfkjhdsflahsdf");
    r8port.setTimeSincePageload(123.12341234);
    r8port.setTimestamp(new DateTime());

    List<R8port> r8ports = Arrays.asList(r8port);

    Future future = storageService.putRecords(r8ports);

    future.get();
  }

  @Test
  public void testUpdateRecords() throws Exception {

  }
}