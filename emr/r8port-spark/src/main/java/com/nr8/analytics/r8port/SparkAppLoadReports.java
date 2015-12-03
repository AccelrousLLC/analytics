package com.nr8.analytics.r8port;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nr8.analytics.r8port.services.dynamo.DynamoR8portStorageService;
import kafka.serializer.StringDecoder;
import org.apache.spark.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class SparkAppLoadReports {

  private static final Logger logger = LoggerFactory.getLogger(SparkAppLoadReports.class);

  public static final String APP_NAME = "R8portLoader";

  public static void main(String[] args){

    String clusterMode = (args.length > 0 && args[0].isEmpty())? args[0] : "local[4]";

    SparkConf conf = new SparkConf().setMaster(clusterMode).setAppName(APP_NAME);

    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));

    HashSet<String> topicsSet = Sets.newHashSet();

    topicsSet.add("user_activity");

    HashMap<String, String> kafkaParams = Maps.newHashMap();

    kafkaParams.put("metadata.broker.list", "localhost:9092");

    JavaPairInputDStream<String, String> r8ports =
        KafkaUtils.createDirectStream(
            streamingContext,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    r8ports
        .groupByKey()
        .map(new Function<Tuple2<String,Iterable<String>>, Object>() {
      @Override
      public Object call(Tuple2<String,Iterable<String>> keyAndValue) throws Exception {

        DynamoR8portStorageService storageService = new DynamoR8portStorageService();

        List<R8port> r8portList = Lists.newArrayList();

        for (String serializeR8port : keyAndValue._2()){
          R8port r8port = JsonUtils.deserialize(serializeR8port, R8port.class);
          r8portList.add(r8port);
        }

        storageService.appendToStorage(r8portList);
        
        return keyAndValue._1();
      }
    }).print();

    streamingContext.start();

    streamingContext.awaitTermination();
  }

  private static Gson getDeserializer(){
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(DateTime.class, new DateTimeDeserializer());
    return gsonBuilder.create();
  }

}
