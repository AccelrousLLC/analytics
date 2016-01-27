package com.nr8.analytics.r8port.services.cassandra;


import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.nr8.analytics.r8port.JsonUtils;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.config.models.CassandraConfig;
import com.nr8.analytics.r8port.services.R8portStorageService;
import org.apache.avro.generic.GenericData;
import org.mortbay.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class CassandraR8portStorageService implements R8portStorageService, Closeable {
  private static Logger sLogger = LoggerFactory.getLogger(CassandraR8portStorageService.class);
  ExecutorService threadPool;
  private String table;
  private Cluster cluster;
  private Session session;

  public CassandraR8portStorageService(CassandraConfig config){
    this.table = config.getTable();
    this.threadPool = Executors.newCachedThreadPool();
    this.cluster = Cluster.builder()
        .addContactPoint(config.getEndpoint())
        .build();
    this.session = this.cluster.connect(config.getKeyspace());

    // Debug.
/*
    Metadata metadata = cluster.getMetadata();
    sLogger.warn(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
    for (Host host: metadata.getAllHosts()) {
      sLogger.warn(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
          host.getDatacenter(), host.getAddress(), host.getRack()));
    }
*/
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

    boolean isStartOfSession = false;

    for (R8port r8port : r8ports){
      if (r8port.getComponent().endsWith("$sessionStart")){
        isStartOfSession = true;
        break;
      }
    }

    if (isStartOfSession) return putRecords(r8ports);
    return updateRecords(r8ports);
  }

  @Override
  public Future<List<R8port>> get(String sessionID) {
    Statement stmt = QueryBuilder
        .select()
        .all()
        .from(this.table)
        .where(QueryBuilder.eq("session", UUID.fromString(sessionID)));
    stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    final ResultSetFuture resultSetFuture = this.session.executeAsync(stmt);

    FutureTask<List<R8port>> result = new FutureTask<>(new Callable<List<R8port>>() {
      @Override
      public List<R8port> call() throws Exception {
        List<R8port> r8ports = new ArrayList<>();
        ResultSet resultSet = resultSetFuture.get();

        for (Row row : resultSet) {
          R8port[] rs = JsonUtils.deserialize(row.get("events", String.class), R8port[].class);
          for (R8port r : rs) {
            r8ports.add(r);
          }
        }
        return r8ports;
      }
    });

    this.threadPool.execute(result);

    return result;
  }

  private static String[] flattenEvents(List<R8port> r8ports) {

    String[] events = new String[r8ports.size()];

    int count = -1;

    for (R8port r8port : r8ports){
      events[++count] = JsonUtils.serialize(r8port);
    }

    return events;
  }

  protected Future putRecords(List<R8port> r8ports) {
    String recordId = r8ports.get(0).getRecordId();
    String events = JsonUtils.serialize(r8ports);
    Statement stmt = QueryBuilder
        .insertInto(this.table)
        .value("session", UUID.fromString(recordId))
        .value("events", events);
    stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    return this.session.executeAsync(stmt);
  }

  protected Future updateRecords(List<R8port> r8ports) {
    return new FutureTask<List<R8port>>(new Callable<List<R8port>>() {
      @Override
      public List<R8port> call() throws Exception {
        return null;
      }
    });
//    UpdateItemRequest request = new UpdateItemRequest();
//    request.setTableName(this.table);
//
//    AttributeValue sessionAttribute = new AttributeValue().withS(r8ports.get(0).getRecordId());
//    Map<String, AttributeValue> key = Maps.newHashMap();
//    key.put("session", sessionAttribute);
//    request.setKey(key);
//
//    Map<String, String> expressionAttributeNames = Maps.newHashMap();
//    expressionAttributeNames.put("#E", "events");
//    request.setExpressionAttributeNames(expressionAttributeNames);
//
//    Map<String, AttributeValue> expressionAttributeValues = Maps.newHashMap();
//    AttributeValue events = new AttributeValue().withSS(flattenEvents(r8ports));
//    expressionAttributeValues.put(":events", events);
//    request.setExpressionAttributeValues(expressionAttributeValues);
//
//    String updateExpression = "ADD #E :events";
//    request.setUpdateExpression(updateExpression);
//
//    return this.client.updateItemAsync(request);
  }
}
