package com.nr8.analytics.r8port;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nr8.analytics.r8port.config.ConfigLoader;
import com.nr8.analytics.r8port.config.ConfigLoaderFactory;
import com.nr8.analytics.r8port.config.ConfigReference;
import com.nr8.analytics.r8port.config.models.*;
import com.nr8.analytics.r8port.services.cassandra.CassandraR8portStorageService;
import com.nr8.analytics.r8port.services.cassandra.CassandraSessionStatsService;
import kafka.admin.AdminUtils;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Future;

public class SessionEndedReportsSparkStream {
  static Logger sLogger = LoggerFactory.getLogger(SessionEndedReportsSparkStream.class);

  public static void main(String[] args){

    final SessionEndedReportsSparkStreamConfig config = getConfigFromArgs(args);

    SparkConf conf =
        new SparkConf()
            .setMaster(config.getClusterMode())
            .setAppName(config.getSparkAppName());

    Map<String, String> sparkConf = config.getSparkConf();
    Iterator<Map.Entry<String, String>> it = sparkConf.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> confEntry = it.next();
      conf.set(confEntry.getKey(), confEntry.getValue());
    }

    JavaStreamingContext streamingContext =
        new JavaStreamingContext(conf, Durations.seconds(config.getBatchingWindow()));

    final KafkaConfig kafkaUserSessionEndConfig = createTopicAndReturnConfig(config.getKafkaUserSessionEndStream());

    HashSet<String> topicsSet = Sets.newHashSet();

    topicsSet.add(kafkaUserSessionEndConfig.getTopicName());

    HashMap<String, String> kafkaParams = Maps.newHashMap();

    kafkaParams.put("metadata.broker.list", kafkaUserSessionEndConfig.getBroker().load(KafkaBrokerConfig.class).get().getBrokers());

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
            int msgCnt = 0;

            for (String msg: keyAndValue._2()) {
              msgCnt++;
            }

            if (msgCnt > 1) {
              sLogger.warn(String.format(
                  "More than one (%d) session end messages detected for session id %s", msgCnt, sessionID));
            }

            CassandraR8portStorageService storageService =
                new CassandraR8portStorageService(config.getCassandraR8portStorage().load(CassandraConfig.class).get());

            Future<List<R8port>> result = storageService.get(sessionID);
            List<R8port> r8ports = result.get(); // blocking

            DateTime startTime = null;
            DateTime endTime = null;
            String userName = null;

            for (R8port r8port : r8ports) {
              if (r8port.getComponent().endsWith("$sessionStart")) {
                startTime = r8port.getTimestamp();
              }
              if (r8port.getComponent().endsWith("$socketDisconnect")) {
                endTime = r8port.getTimestamp();
              }

              String eventUserName = r8port.getUsername();
              if (userName != null && !userName.equals(eventUserName)) {
                sLogger.warn("Multiple session events have different user names");
              }
              userName = eventUserName;
            }

            if (startTime == null || endTime == null) {
              sLogger.warn(String.format("Missing %s event for session ID %s",
                  (startTime == null ? "sessionStart" : "socketDisconnect"), sessionID));
              return sessionID;
            }

            Nr8SessionStats session = new Nr8SessionStats(sessionID, startTime, endTime, userName);
            CassandraSessionStatsService sessionStatsService =
                new CassandraSessionStatsService(config.getCassandraSessionStats().load(CassandraConfig.class).get());
            sessionStatsService.put(session);

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

  private static SessionEndedReportsSparkStreamConfig getConfigFromArgs(String[] args){

    String loaderURI = args[0];
    String environment = args[1];

    ConfigLoader loader = ConfigLoaderFactory.getLoader(loaderURI).get();

    return loader.getConfig(environment, "spark.session-ended-r8ports", SessionEndedReportsSparkStreamConfig.class).get();
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
