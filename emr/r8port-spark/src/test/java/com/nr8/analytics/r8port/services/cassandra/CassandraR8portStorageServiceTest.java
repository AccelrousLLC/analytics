package com.nr8.analytics.r8port.services.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.JsonObject;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.config.models.CassandraConfig;
import org.joda.time.DateTime;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class CassandraR8portStorageServiceTest {
  private static Logger sLogger = LoggerFactory.getLogger(CassandraR8portStorageServiceTest.class);
  private static final String END_POINT = "127.0.0.1";
  private static final String KEYSPACE = "reports_test";
  private static final String TABLE = "r8ports";
  private static final String KEYSPACE_TABLE = KEYSPACE + "." + TABLE;

  private static final String CREATE_KEYSPACE = "CREATE KEYSPACE " + KEYSPACE
      + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};";
  private static final String DROP_KEYSPACE = "DROP KEYSPACE " + KEYSPACE;

  private static final String CREATE_TABLE = "CREATE TABLE " + KEYSPACE_TABLE
      + "(session uuid PRIMARY KEY, events set<text>);";
  private static final String DROP_TABLE = "DROP TABLE " + KEYSPACE_TABLE + ";";

  private static Cluster sCluster;
  private static Session sSession;
  private CassandraR8portStorageService storageService;

  @BeforeClass
  public static void classSetUp() {
    sCluster = Cluster.builder()
        .addContactPoint(END_POINT)
        .build();
    sSession = sCluster.connect();
    sSession.execute(CREATE_KEYSPACE);
  }

  @AfterClass
  public static void classTearDown() {
    sSession.execute(DROP_KEYSPACE);
    sCluster.close();
  }

  @Before
  public void setUp() {
    sSession.execute(CREATE_TABLE);

    CassandraConfig config = new CassandraConfig();
    config.setKeyspace(KEYSPACE);
    config.setEndpoint(END_POINT);
    config.setTable(TABLE);
    this.storageService = new CassandraR8portStorageService(config);
  }

  @After
  public void tearDown() {
    sSession.execute(DROP_TABLE);

    try {
      this.storageService.close();
    } catch (IOException e) {
      sLogger.error("IOException while closing Cassandra storage service");
    }
  }

  private static String getRandomString(int len) {
    String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder();
    Random random = new Random();

    for (int i = 0; i < len; i++) {
      int rand = random.nextInt(chars.length());
      sb.append(chars.substring(rand, rand+1));
    }

    return sb.toString();
  }

  private static R8port getR8port(String uuid) {
    R8port r8port = new R8port();

    r8port.setEventData(new JsonObject());
    r8port.setReportDate(new DateTime());
    r8port.setUserAgent(getRandomString(25));
    r8port.setUsername(getRandomString(10));
    r8port.setComponent(getRandomString(10));
    r8port.setRecordId(uuid);
    r8port.setSid(getRandomString(20));
    r8port.setTimeSincePageload(123.12341234);
    r8port.setTimestamp(new DateTime());

    return r8port;
  }

  @Test
  public void testAppendToStorage() throws Exception {
    R8port r8port = getR8port(UUID.randomUUID().toString());
    List<R8port> r8ports = Arrays.asList(r8port);
    Future future = this.storageService.appendToStorage(r8ports);
    future.get();
  }

  @Test
  public void testGetOne() throws Exception {
    String uuid = UUID.randomUUID().toString();
    R8port r8port = getR8port(uuid);
    List<R8port> r8ports = Arrays.asList(r8port);
    this.storageService.appendToStorage(r8ports).get();
    Future<List<R8port>> result = this.storageService.get(uuid);
    List<R8port> queriedR8ports = result.get();

    assertEquals(r8ports.size(), queriedR8ports.size());

    R8port queriedR8port = queriedR8ports.get(0);
    assertEquals(r8ports.size(), queriedR8ports.size());
    assertEquals(r8port.getRecordId(), queriedR8port.getRecordId());
    assertEquals(r8port.getReportDate(), queriedR8port.getReportDate());
    assertEquals(r8port.getUsername(), queriedR8port.getUsername());
  }

  @Test
  public void testGetMultiple() throws Exception {
    String uuid = UUID.randomUUID().toString();
    ArrayList<R8port> r8ports = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      r8ports.add(getR8port(uuid));
    }

    this.storageService.appendToStorage(r8ports).get();
    Future<List<R8port>> result = this.storageService.get(uuid);
    List<R8port> queriedR8ports = result.get();
    assertEquals(r8ports.size(), queriedR8ports.size());
  }

  @Test
  public void testAppendDuplicates() throws Exception {
    String uuid = UUID.randomUUID().toString();
    R8port r8port = getR8port(uuid);
    ArrayList<R8port> r8ports = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      r8ports.add(r8port);
    }

    assertEquals(20, r8ports.size());

    this.storageService.appendToStorage(r8ports).get();
    Future<List<R8port>> result = this.storageService.get(uuid);
    List<R8port> queriedR8ports = result.get();
    assertEquals(1, queriedR8ports.size());
  }
}
