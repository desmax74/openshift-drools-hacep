/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kie.u212.core;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

  public static final String MASTER_TOPIC = "master";
  public static final String USERS_INPUT_TOPIC = "users";
  public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST";
  public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT";
  public static final String BROKER_URL = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST);
  public static final String BROKER_PORT = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_PORT);
  public static final String GROUP = "drools";//@ŢODO
  public static final int DEFAULT_POLL_SIZE = 1000;
  public static final int LOOP_DURATION = -1;
  public static final boolean DEFAULT_COMMIT_SYNC = true;
  private static final Logger logger = LoggerFactory.getLogger(Config.class);


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
    properties.put("group.id", GROUP);//@TODO
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
