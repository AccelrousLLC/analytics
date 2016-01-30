package com.nr8.analytics.r8port.services.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.Schema;
import com.google.gson.JsonObject;
import com.nr8.analytics.r8port.JsonUtils;
import com.nr8.analytics.r8port.Nr8SessionStats;
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

public class CassandraSessionStatsServiceTest {
  private static Logger sLogger = LoggerFactory.getLogger(CassandraSessionStatsServiceTest.class);
  private static final String END_POINT = "127.0.0.1";
  private static final String KEYSPACE = "reports_test";
  private static final String TABLE = "session_stats";
  private static final String KEYSPACE_TABLE = KEYSPACE + "." + TABLE;

  private static final String CREATE_KEYSPACE = "CREATE KEYSPACE " + KEYSPACE
      + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};";
  private static final String DROP_KEYSPACE = "DROP KEYSPACE " + KEYSPACE;

  private static final String CREATE_TABLE = "CREATE TABLE " + KEYSPACE_TABLE
      + "(session uuid PRIMARY KEY, timestamp timestamp, duration bigint, username text);";
  private static final String DROP_TABLE = "DROP TABLE " + KEYSPACE_TABLE + ";";

  private static Cluster sCluster;
  private static Session sSession;
  private CassandraSessionStatsService sessionStatsService;

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
    this.sessionStatsService = new CassandraSessionStatsService(config);
  }

  @After
  public void tearDown() {
    sSession.execute(DROP_TABLE);

    try {
      this.sessionStatsService.close();
    } catch (IOException e) {
      sLogger.error("IOException while closing Cassandra storage service");
    }
  }

  @Test
  public void testPut() throws Exception {
    String sessionId = UUID.randomUUID().toString();
    DateTime startTime = new DateTime();
    DateTime endTime = new DateTime();
    String username = "test_user";
    
    Nr8SessionStats stats = new Nr8SessionStats(sessionId, startTime, endTime, username);
    this.sessionStatsService.put(stats).get();

    ResultSet resultSet = sSession.execute("SELECT * FROM " + KEYSPACE_TABLE);
    List<Row> rows = resultSet.all();
    assertEquals(1, rows.size());

    Row row = rows.get(0);
    String queriedSessionId = row.getUUID(("session")).toString();
    assertEquals(queriedSessionId, sessionId);
  }
}
