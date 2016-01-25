package com.nr8.analytics.r8port.services.cassandra;

import com.google.gson.JsonObject;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.config.models.CassandraConfig;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

public class CassandraR8portStorageServiceTest {
  private CassandraR8portStorageService getStorageService() {
    CassandraConfig config = new CassandraConfig();
    config.setKeyspace("reports_development");
    config.setEndpoint("127.0.0.1");
    config.setTable("r8ports");
    return new CassandraR8portStorageService(config);
  }

  @Test
  public void testPutRecords() throws Exception {
    CassandraR8portStorageService storageService = getStorageService();
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
}
