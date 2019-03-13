package org.kie.u212.core;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

  public static final String MASTER_TOPIC = "master.events";
  public static final String USERS_INPUT_TOPIC = "users.input.events";
  public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST";
  public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT";
  public static final String BROKER_URL = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST);
  public static final String BROKER_PORT = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT);
  public static final String GROUP = "group-1";//@ŢODO
  private static final Logger logger = LoggerFactory.getLogger(Config.class);

  public static Properties getConfig(String groupId,
                                     String valueSerializerClassName,
                                     boolean autoCommit) {
    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", BROKER_URL + ":" + BROKER_PORT);
    producerProperties.put("group.id", groupId);
    producerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    producerProperties.put("value.deserializer", valueSerializerClassName);
    producerProperties.setProperty("enable.auto.commit",
                                   String.valueOf(autoCommit));
    logConfig(producerProperties);
    return producerProperties;
  }

  public static String getBotStrapServers() {
    //@TODO
    StringBuilder sb = new StringBuilder();
    sb.append(Config.BROKER_URL).append(":").append(Config.BROKER_PORT)
            .append(",").append("my-cluster-kafka-brokers.my-kafka-project.svc").append(":9091")
            .append(",").append("my-cluster-kafka-brokers.my-kafka-project.svc").append(":9092")
            .append(",").append("my-cluster-kafka-brokers.my-kafka-project.svc").append(":9093");
    return sb.toString();
  }

  public static Properties getDefaultConfig() {
    Properties properties = new Properties();
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.kie.u212.consumer.EventJsonSerializer");
    properties.put("bootstrap.servers", getBotStrapServers());
    properties.put("group.id", "1");//@TODO
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.kie.u212.producer.EventJsonDeserializer");
    properties.setProperty("enable.auto.commit", String.valueOf(true));//@TODO
    logConfig(properties);
    return properties;
  }

  private static void logConfig(Properties producerProperties) {
    if (logger.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<Object, Object> entry : producerProperties.entrySet()) {
        sb.append(entry.getKey().toString()).append(":").append(entry.getValue());
      }
      logger.info(sb.toString());
    }
  }
}
