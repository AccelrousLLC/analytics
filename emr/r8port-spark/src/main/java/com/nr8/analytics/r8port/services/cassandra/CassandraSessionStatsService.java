package com.nr8.analytics.r8port.services.cassandra;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.nr8.analytics.r8port.Nr8SessionStats;
import com.nr8.analytics.r8port.config.models.CassandraConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Future;

public class CassandraSessionStatsService implements Closeable {
  private String table;
  private Cluster cluster;
  private Session session;

  public CassandraSessionStatsService(CassandraConfig config) {
    this.table = config.getTable();
    this.cluster = Cluster.builder()
        .addContactPoint(config.getEndpoint())
        .build();
    this.session = this.cluster.connect(config.getKeyspace());
  }

  @Override
  public void close() throws IOException {
    if (this.cluster != null) {
      this.cluster.close();
    }
  }

  public Future put(Nr8SessionStats session) {
    Statement stmt = QueryBuilder
        .insertInto(this.table)
        .value("session", UUID.fromString(session.sessionID))
        .value("timestamp", session.startTime.toDate())
        .value("duration", session.duration)
        .value("username", session.username == null ? "<anonymous>" : session.username)
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    return this.session.executeAsync(stmt);
  }
}
