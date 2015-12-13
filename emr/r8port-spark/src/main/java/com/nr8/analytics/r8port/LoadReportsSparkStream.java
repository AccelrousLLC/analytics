package com.nr8.analytics.r8port;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nr8.analytics.r8port.config.ConfigLoader;
import com.nr8.analytics.r8port.config.ConfigLoaderFactory;
import com.nr8.analytics.r8port.config.models.DynamoConfig;
import com.nr8.analytics.r8port.config.models.KafkaConfig;
import com.nr8.analytics.r8port.config.models.LoadReportsSparkStreamConfig;
import com.nr8.analytics.r8port.services.dynamo.DynamoR8portStorageService;
import kafka.admin.AdminUtils;
import kafka.common.Topic;
import kafka.common.TopicExistsException;
import kafka.serializer.StringDecoder;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.logging.Log;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

public class LoadReportsSparkStream {

  public static void main(String[] args){

    final LoadReportsSparkStreamConfig config = getConfigFromArgs(args);

    SparkConf conf =
        new SparkConf()
              .setMaster(config.getClusterMode())
              .setAppName(config.getSparkAppName());

    JavaStreamingContext streamingContext =
        new JavaStreamingContext(conf, Durations.seconds(config.getBatchingWindow()));

    KafkaConfig kafkaConfig = config.getKafka().load(KafkaConfig.class).get();

    createKafkaTopic(kafkaConfig);

    HashSet<String> topicsSet = Sets.newHashSet();

    topicsSet.add(config.getR8portKafkaTopic());

    HashMap<String, String> kafkaParams = Maps.newHashMap();

    kafkaParams.put("metadata.broker.list", kafkaConfig.getBrokers());

    JavaPairInputDStream<String, String> r8ports =
        KafkaUtils.createDirectStream(
            streamingContext,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    r8ports
        .groupByKey()
        .map(new Function<Tuple2<String,Iterable<String>>, Object>() {
      @Override
      public Object call(Tuple2<String,Iterable<String>> keyAndValue) throws Exception {

        DynamoR8portStorageService storageService =
            new DynamoR8portStorageService(config.getDynamo().load(DynamoConfig.class).get());

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

  private static LoadReportsSparkStreamConfig getConfigFromArgs(String[] args){

    String loaderURI = args[0];
    String environment = args[1];

    ConfigLoader loader = ConfigLoaderFactory.getLoader(loaderURI).get();

    return loader.getConfig(environment, "spark.load-r8ports", LoadReportsSparkStreamConfig.class).get();
  }

  private static boolean createKafkaTopic(KafkaConfig config){
    ZkClient client = new ZkClient(config.getZookeepers());
    String topicName = config.getTopicName();

    if (topicName == null) {
      System.out.println("Topic name is null, skipping topic creation.");
      return false;
    }

    if (AdminUtils.topicExists(client, topicName)) {
      System.out.println(String.format("Topic %s exists already, skipping creation.", topicName));
      return false;
    }

    AdminUtils.createTopic(
            client, topicName, config.getPartitions(), config.getReplication(), new Properties());

    return true;
  }
}
