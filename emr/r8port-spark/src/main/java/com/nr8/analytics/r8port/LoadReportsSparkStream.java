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
import kafka.Kafka;
import kafka.admin.AdminUtils;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.ZkClient;
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

    KafkaConfig kafkaConfig = config.getKafka().get(KafkaConfig.class).get();

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
            new DynamoR8portStorageService(config.getDynamo().get(DynamoConfig.class).get());

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

  private static void createKafkaTopic(KafkaConfig config){
    ZkClient client = new ZkClient(config.getZookeepers());
    AdminUtils.createTopic(
        client, config.getTopicName(), config.getPartitions(), config.getReplication(), new Properties());
  }
}
