package com.nr8.analytics.r8port;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

public class SparkAppLoadReports {

  private static final Logger logger = LoggerFactory.getLogger(SparkAppLoadReports.class);

  public static final String APP_NAME = "R8portLoader";
  public static final Gson gson = getDeserializer();

  public static void main(String[] args){

    String clusterMode = (args.length > 0 && args[0].isEmpty())? args[0] : "local[4]";

    String kinesisStream = System.getenv("KINESIS_STREAM");

    if (kinesisStream == null || kinesisStream.isEmpty()) kinesisStream = "user_activity_development";

    SparkConf conf = new SparkConf().setMaster(clusterMode).setAppName(APP_NAME);

    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));

    /*
     JavaPairReceiverInputDStream<String, String> directKafkaStream =
     KafkaUtils.createDirectStream(streamingContext,
         [key class], [value class], [key decoder class], [value decoder class],
         [map of Kafka parameters], [set of topics to consume]);
    */

    HashSet<String> topicsSet = Sets.newHashSet();

    topicsSet.add("user_activity");

    HashMap<String, String> kafkaParams = Maps.newHashMap();

    kafkaParams.put("metadata.broker.list", "localhost:9092");

    JavaPairInputDStream<String, String> r8ports =
        KafkaUtils.createDirectStream(
            streamingContext,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    //r8ports.groupByKey().count().print();

    r8ports.map(new Function<Tuple2<String,String>, Object>() {
      @Override
      public Object call(Tuple2<String, String> keyAndValue) throws Exception {

        R8port r8port = null;

        try {
          r8port = gson.fromJson(keyAndValue._2(), R8port.class);
        } catch (Exception e){
          logger.error("Could not deserialize r8port payload: {}", e);
        }

        return keyAndValue._2();
      }
    }).print();

//    JavaReceiverInputDStream<byte[]> r8ports = KinesisUtils.createStream(
//        streamingContext,
//        APP_NAME,
//        kinesisStream,
//        KINESIS_ENDPOINT,
//        KINESIS_AWS_REGION,
//        InitialPositionInStream.LATEST,
//        Durations.seconds(2),
//        StorageLevel.MEMORY_AND_DISK_2()
//    );
//
//    r8ports.map(new Function<byte[], Object>() {
//      @Override
//      public Object call(byte[] v1) throws Exception {
//        logger.info("Got a record on the Kinesis stream.");
//        return true;
//      }
//    }).print();

    streamingContext.start();

    streamingContext.awaitTermination();
  }

  private static Gson getDeserializer(){
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(DateTime.class, new DateTimeDeserializer());
    return gsonBuilder.create();
  }

}
