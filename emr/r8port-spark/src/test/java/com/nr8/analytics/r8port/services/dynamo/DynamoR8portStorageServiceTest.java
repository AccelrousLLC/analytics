package com.nr8.analytics.r8port.services.dynamo;

import com.clearspring.analytics.util.Lists;
import com.google.gson.JsonObject;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.config.models.DynamoConfig;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class DynamoR8portStorageServiceTest {
  private DynamoR8portStorageService getStorageService() {
    DynamoConfig config = new DynamoConfig();
    config.setEndpoint("dynamodb.us-west-2.amazonaws.com");
    config.setTable("r8ports-development"); // need a test table?
    return new DynamoR8portStorageService(config);
  }

  @Test
  public void testPutRecords() throws Exception {
    DynamoR8portStorageService storageService = getStorageService();

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

  @Test
  public void testGet() throws Exception {
    DynamoR8portStorageService storageService = getStorageService();

    R8port r8port = new R8port();
    String recordId = "98382DA9-9073-4B01-9F96-E8CE931D2774";
    DateTime date = new DateTime();
    String userAgent = "DynamoR8portStorageServiceTest.testGet()";
    String username = "arno";
    String component = "spark-unit-test";

    r8port.setEventData(new JsonObject());
    r8port.setReportDate(date);
    r8port.setUserAgent(userAgent);
    r8port.setUsername(username);
    r8port.setComponent(component);
    r8port.setRecordId(recordId);
    r8port.setSid("lklaksdjflkasdashdfihqweoifhkjnfkjhdsflahsdf");
    r8port.setTimeSincePageload(123.12341234);
    r8port.setTimestamp(new DateTime());
    storageService.putRecords(Arrays.asList(r8port)).get();

    Future<List<R8port>> future = storageService.get(recordId);
    List<R8port> r8ports = future.get();

    assertEquals(1, r8ports.size());
    R8port retrievedR8port = r8ports.get(0);

    assertEquals(recordId, retrievedR8port.getRecordId());
    assertEquals(date, retrievedR8port.getReportDate());
    assertEquals(userAgent, retrievedR8port.getUserAgent());
    assertEquals(username, retrievedR8port.getUsername());
    assertEquals(component, retrievedR8port.getComponent());
  }
}