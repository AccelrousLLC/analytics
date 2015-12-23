package com.nr8.analytics.r8port.services.dynamo;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.Maps;
import com.nr8.analytics.r8port.JsonUtils;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.config.models.DynamoConfig;
import com.nr8.analytics.r8port.services.R8portStorageService;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class DynamoR8portStorageService implements R8portStorageService {

  protected AmazonDynamoDBAsyncClient client;
  protected String table;

  public DynamoR8portStorageService(DynamoConfig config){
    client = new AmazonDynamoDBAsyncClient();
    client.setEndpoint(config.getEndpoint());
    this.table = config.getTable();
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
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("session", new AttributeValue().withS(sessionID));

    final Future<GetItemResult> resultFuture = this.client.getItemAsync(new GetItemRequest(this.table, key));

    FutureTask<List<R8port>> futureTask = new FutureTask<>(new Callable<List<R8port>>() {
      @Override
      public List<R8port> call() throws Exception {
        GetItemResult result = resultFuture.get(); // blocking
        Map<String, AttributeValue> item = result.getItem();
        AttributeValue eventsValue = item.get("events");
        List<String> events = eventsValue.getSS();
        List<R8port> r8ports = new ArrayList<>(events.size());

        for (String event : events) {
          r8ports.add(JsonUtils.deserialize(event, R8port.class));
        }

        return r8ports;
      }
    });

    // May need thread pool.
    // Pb is that the DynamoR8portStorageService will need to be shutdown
    // after use every time.
    new Thread(futureTask).start();

    return futureTask;
  }

  static String[] flattenEvents(List<R8port> r8ports){

    String[] events = new String[r8ports.size()];

    int count = -1;

    for (R8port r8port : r8ports){
      events[++count] = JsonUtils.serialize(r8port);
    }

    return events;
  }

  protected Future putRecords(List<R8port> r8ports){

    PutItemRequest request = new PutItemRequest();

    Map<String, AttributeValue> item = Maps.newHashMap();

    item.put("session", new AttributeValue().withS(r8ports.get(0).getRecordId()));

    item.put("events", new AttributeValue().withSS(flattenEvents(r8ports)));

    request.setTableName(this.table);
    request.setItem(item);

    return this.client.putItemAsync(request);
  }

  protected Future updateRecords(List<R8port> r8ports){

    AttributeValue sessionAttribute = new AttributeValue().withS(r8ports.get(0).getRecordId());

    UpdateItemRequest request = new UpdateItemRequest();

    request.setTableName(this.table);

    Map<String, AttributeValue> key = Maps.newHashMap();

    key.put("session", sessionAttribute);

    request.setKey(key);

    AttributeValue events = new AttributeValue().withSS(flattenEvents(r8ports));

    Map<String, String> expressionAttributeNames = Maps.newHashMap();

    expressionAttributeNames.put("#E", "events");

    request.setExpressionAttributeNames(expressionAttributeNames);

    Map<String, AttributeValue> expressionAttributeValues = Maps.newHashMap();

    expressionAttributeValues.put(":events", events);

    request.setExpressionAttributeValues(expressionAttributeValues);

    String updateExpression = "ADD #E :events";

    request.setUpdateExpression(updateExpression);

    return this.client.updateItemAsync(request);
  }
}
