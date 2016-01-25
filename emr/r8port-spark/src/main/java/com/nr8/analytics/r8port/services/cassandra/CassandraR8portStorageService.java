package com.nr8.analytics.r8port.services.cassandra;


import com.datastax.driver.core.*;
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
    Metadata metadata = cluster.getMetadata();
    sLogger.warn(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
    for (Host host: metadata.getAllHosts()) {
      sLogger.warn(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
          host.getDatacenter(), host.getAddress(), host.getRack()));
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
    return new FutureTask<List<R8port>>(new Callable<List<R8port>>() {
      @Override
      public List<R8port> call() throws Exception {
        return null;
      }
    });
//    Map<String, AttributeValue> key = new HashMap<>();
//    key.put("session", new AttributeValue().withS(sessionID));
//
//    final Future<GetItemResult> resultFuture = this.client.getItemAsync(new GetItemRequest(this.table, key));
//
//    FutureTask<List<R8port>> futureTask = new FutureTask<>(new Callable<List<R8port>>() {
//      @Override
//      public List<R8port> call() throws Exception {
//        GetItemResult result = resultFuture.get(); // blocking
//        Map<String, AttributeValue> item = result.getItem();
//        AttributeValue eventsValue = item.get("events");
//        List<String> events = eventsValue.getSS();
//        List<R8port> r8ports = new ArrayList<>(events.size());
//
//        for (String event : events) {
//          r8ports.add(JsonUtils.deserialize(event, R8port.class));
//        }
//
//        return r8ports;
//      }
//    });
//
//    // May need thread pool.
//    // Pb is that the DynamoR8portStorageService will need to be shutdown
//    // after use every time.
//    new Thread(futureTask).start();
//
//    return futureTask;
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
    return this.session.executeAsync("INSERT INTO " + this.table + " (session, events) VALUES (?, ?);", recordId, events);

//    PutItemRequest request = new PutItemRequest();
//
//    Map<String, AttributeValue> item = Maps.newHashMap();
//
//    item.put("session", new AttributeValue().withS(r8ports.get(0).getRecordId()));
//
//    item.put("events", new AttributeValue().withSS(flattenEvents(r8ports)));
//
//    request.setTableName(this.table);
//    request.setItem(item);
//
//    return this.client.putItemAsync(request);
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
