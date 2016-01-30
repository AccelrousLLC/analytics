package com.nr8.analytics.r8port.services.dynamo;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.nr8.analytics.r8port.Nr8SessionStats;
import com.nr8.analytics.r8port.config.models.DynamoConfig;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class DynamoSessionStatsService {

  protected AmazonDynamoDBAsyncClient client;
  protected String table;

  public DynamoSessionStatsService(DynamoConfig config){
    client = new AmazonDynamoDBAsyncClient();
    client.setEndpoint(config.getEndpoint());
    this.table = config.getTable();
  }

  public Future<PutItemResult> put(Nr8SessionStats session){
    PutItemRequest request = new PutItemRequest();
    Map<String, AttributeValue> item = new HashMap<>();
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();

    item.put("session", new AttributeValue().withS(session.sessionID));
    item.put("timestamp", new AttributeValue().withS(fmt.print(session.startTime)));
    item.put("duration", new AttributeValue().withN(Long.toString(session.duration)));
    item.put("username", new AttributeValue().withS((session.username == null) ? "<anonymous>" : session.username));

    request.setTableName(this.table);
    request.setItem(item);

    return this.client.putItemAsync(request);
  }
}
