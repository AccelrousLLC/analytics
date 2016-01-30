package com.nr8.analytics.r8port.services.cassandra;


import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.nr8.analytics.r8port.JsonUtils;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.config.models.CassandraConfig;
import com.nr8.analytics.r8port.services.R8portStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

public class CassandraR8portStorageService implements R8portStorageService, Closeable {
  private static Logger sLogger = LoggerFactory.getLogger(CassandraR8portStorageService.class);
  ExecutorService threadPool;
  private String table;
  private Cluster cluster;
  private Session session;
  private static final boolean DEBUG = false;

  public CassandraR8portStorageService(CassandraConfig config){
    this.table = config.getTable();
    this.threadPool = Executors.newCachedThreadPool();
    this.cluster = Cluster.builder()
        .addContactPoint(config.getEndpoint())
        .build();
    this.session = this.cluster.connect(config.getKeyspace());

    if (DEBUG) {
      Metadata metadata = cluster.getMetadata();
      sLogger.warn(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
      for (Host host : metadata.getAllHosts()) {
        sLogger.warn(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
            host.getDatacenter(), host.getAddress(), host.getRack()));
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (this.threadPool != null) {
      this.threadPool.shutdown();
    }
    if (this.cluster != null) {
      this.cluster.close();
    }
  }

  @Override
  public Future appendToStorage(List<R8port> r8ports) {
    String recordId = r8ports.get(0).getRecordId();
    Update up = QueryBuilder
        .update(this.table);

    for (R8port r8port : r8ports) {
      up.with(QueryBuilder.append("events", JsonUtils.serialize(r8port)));
    }

    up.where(QueryBuilder.eq("session", UUID.fromString(recordId)))
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    return this.session.executeAsync(up);
  }

  @Override
  public Future<List<R8port>> get(String sessionID) {
    Statement stmt = QueryBuilder
        .select()
        .all()
        .from(this.table)
        .where(QueryBuilder.eq("session", UUID.fromString(sessionID)))
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    final ResultSetFuture resultSetFuture = this.session.executeAsync(stmt);

    FutureTask<List<R8port>> result = new FutureTask<>(new Callable<List<R8port>>() {
      @Override
      public List<R8port> call() throws Exception {
        List<R8port> r8ports = new ArrayList<>();
        ResultSet resultSet = resultSetFuture.get();

        for (Row row : resultSet) {
          Set<String> events = row.getSet("events", String.class);
          for (String event : events) {
            r8ports.add(JsonUtils.deserialize(event, R8port.class));
          }
        }
        return r8ports;
      }
    });

    this.threadPool.execute(result);

    return result;
  }
}
