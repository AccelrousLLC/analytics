package com.nr8.analytics.r8port.services.dynamo;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.Maps;
import com.nr8.analytics.r8port.JsonUtils;
import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.SessionLog;
import com.nr8.analytics.r8port.config.models.DynamoConfig;
import com.nr8.analytics.r8port.services.R8portStorageService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

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
  public SessionLog get(String sessionID) {
    return null;
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
