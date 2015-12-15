package com.nr8.analytics.r8port;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nr8.analytics.r8port.config.ConfigLoader;
import com.nr8.analytics.r8port.config.ConfigLoaderFactory;
import com.nr8.analytics.r8port.config.ConfigReference;
import com.nr8.analytics.r8port.config.models.DynamoConfig;
import com.nr8.analytics.r8port.config.models.KafkaBrokerConfig;
import com.nr8.analytics.r8port.config.models.KafkaConfig;
import com.nr8.analytics.r8port.config.models.LoadReportsSparkStreamConfig;
import com.nr8.analytics.r8port.services.dynamo.DynamoR8portStorageService;
import com.nr8.analytics.r8port.services.kafka.KafkaProducerService;
import kafka.admin.AdminUtils;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

public class LoadReportsSparkStream {
  static Logger sLogger = LoggerFactory.getLogger(LoadReportsSparkStream.class);

  public static void main(String[] args){

    final LoadReportsSparkStreamConfig config = getConfigFromArgs(args);

    SparkConf conf =
        new SparkConf()
              .setMaster(config.getClusterMode())
              .setAppName(config.getSparkAppName());

    JavaStreamingContext streamingContext =
        new JavaStreamingContext(conf, Durations.seconds(config.getBatchingWindow()));

    KafkaConfig kafkaUserActivityStreamConfig = createTopicAndReturnConfig(config.getKafkaUserActivityStream());
    final KafkaConfig kafkaUserSessionEndConfig = createTopicAndReturnConfig(config.getKafkaUserSessionEndStream());

    HashSet<String> topicsSet = Sets.newHashSet();

    topicsSet.add(config.getR8portKafkaTopic());

    HashMap<String, String> kafkaParams = Maps.newHashMap();

    kafkaParams.put("metadata.broker.list", kafkaUserActivityStreamConfig.getBroker().load(KafkaBrokerConfig.class).get().getBrokers());

    JavaPairInputDStream<String, String> r8ports =
        KafkaUtils.createDirectStream(
            streamingContext,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    r8ports
        .groupByKey()
        .map(new Function<Tuple2<String,Iterable<String>>, Object>() {
      @Override
      public Object call(Tuple2<String,Iterable<String>> keyAndValue) throws Exception {

        String sessionID = keyAndValue._1();

        DynamoR8portStorageService storageService =
            new DynamoR8portStorageService(config.getDynamo().load(DynamoConfig.class).get());

        KafkaProducerService kafkaProducerService = new KafkaProducerService(kafkaUserSessionEndConfig);

        List<R8port> r8portList = Lists.newArrayList();

        for (String serializeR8port : keyAndValue._2()){
          R8port r8port = JsonUtils.deserialize(serializeR8port, R8port.class);
          r8portList.add(r8port);

          sLogger.warn("Received message for component: {}", r8port.getComponent());

          if (r8port.getComponent().endsWith("$socketDisconnect")){
            kafkaProducerService.send(sessionID, sessionID);
            sLogger.info("Socket Disconnect event detected for {}", sessionID);
          }
        }

        storageService.appendToStorage(r8portList);

        return sessionID;
      }
    }).print();

    streamingContext.start();

    streamingContext.awaitTermination();
  }

  private static KafkaConfig createTopicAndReturnConfig(ConfigReference<KafkaConfig> reference){
    KafkaConfig config = reference.load(KafkaConfig.class).get();
    createKafkaTopic(config);
    return config;
  }

  private static LoadReportsSparkStreamConfig getConfigFromArgs(String[] args){

    String loaderURI = args[0];
    String environment = args[1];

    ConfigLoader loader = ConfigLoaderFactory.getLoader(loaderURI).get();

    return loader.getConfig(environment, "spark.load-r8ports", LoadReportsSparkStreamConfig.class).get();
  }

  private static boolean createKafkaTopic(KafkaConfig config){

    KafkaBrokerConfig brokerConfig = config.getBroker().load(KafkaBrokerConfig.class).get();

    ZkClient client = new ZkClient(brokerConfig.getZookeepers(), 10000, 10000, ZKStringSerializer$.MODULE$);

    String topicName = config.getTopicName();

    if (topicName == null) {
      sLogger.warn("Topic name is null, skipping topic creation.");
      return false;
    }

    if (AdminUtils.topicExists(client, topicName)) {
      sLogger.warn(String.format("Topic %s exists already, skipping creation.", topicName));
      return false;
    }

    try {

      AdminUtils.createTopic(
          client, topicName, config.getPartitions(), config.getReplication(), new Properties());

    } catch (kafka.common.TopicExistsException e){
      sLogger.info("Topic {} already exists, ignoring...", topicName);
    }

    return true;
  }
}
