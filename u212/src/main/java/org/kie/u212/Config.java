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
package org.kie.u212;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

    public static final String CONTROL_TOPIC = "control";
    public static final String EVENTS_TOPIC = "events";
    public static final String SNAPSHOT_TOPIC = "snapshot";
    public static final String ITERATION_BETWEEN_SNAPSHOT = "iteration.between.snapshot";
    public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST";
    public static final String BROKER_URL = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST);
    public static final int DEFAULT_POLL_TIMEOUT_MS = 1000;
    public static final int LOOP_DURATION = -1;
    public static final boolean DEFAULT_COMMIT_SYNC = true;
    public static final boolean SUBSCRIBE_MODE = false;
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private static Properties config;
    private static Properties consumerConf, producerConf, snapshotConsumerConf, snapshotProducerConf;
    private static final String CONSUMER_CONF = "consumer.properties";
    private static final String PRODUCER_CONF = "producer.properties";
    private static final String CONF = "infra.properties";
    private static final String SNAPSHOT_CONSUMER_CONF = "snapshot_consumer.properties";
    private static final String SNAPSHOT_PRODUCER_CONF = "snapshot_producer.properties";

    public static String getBotStrapServers() {
        StringBuilder sb = new StringBuilder();
        sb.append(Config.BROKER_URL).append(":9092");
        //append("my-cluster-kafka-bootstrap.my-kafka-project.svc:9092");//plain
        //.append(",").append("my-cluster-kafka-brokers.my-kafka-project.svc").append(":9093");//tls
        return sb.toString();
    }

    public static Properties getDefaultConfig() {
        return getDefaultConfigFromProps(CONF);
    }

    public static Properties getConsumerConfig() {
        if(consumerConf == null){
            consumerConf = getDefaultConfigFromProps(CONSUMER_CONF);
        }
        return consumerConf;
    }

    public static Properties getProducerConfig() {
        if(producerConf == null){
            producerConf = getDefaultConfigFromProps(PRODUCER_CONF);
        }
        return producerConf;
    }

    public static Properties getSnapshotConsumerConfig() {
        if(snapshotConsumerConf == null){
            snapshotConsumerConf = getDefaultConfigFromProps(SNAPSHOT_CONSUMER_CONF);
        }
        return snapshotConsumerConf;
    }

    public static Properties getSnapshotProducerConfig() {
        if(snapshotProducerConf == null){
            snapshotProducerConf = getDefaultConfigFromProps(SNAPSHOT_PRODUCER_CONF);
        }
        return snapshotProducerConf;
    }

    public static Properties getStatic() {
        if(config == null) {
            config = new Properties();
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("value.serializer", "org.kie.u212.serializer.EventJsonSerializer");
            config.put("bootstrap.servers", getBotStrapServers());
            config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            config.put("value.deserializer", "org.kie.u212.producer.EventJsonDeserializer");
            config.put("max.poll.interval.ms", "10000");//time to discover the new consumer after a changetopic default 5 min 300000
            config.put("batch.size", "16384");
            config.put("enable.auto.commit", "false");
            config.put("metadata.max.age.ms", "10000");
            config.put("iteration.between.snapshot", "10");
            //config.setProperty("enable.auto.commit", String.valueOf(true));
        }
        //logConfig(props);
        return config;
    }



    public static Properties getDefaultConfigFromProps(String fileName) {//@TODO

            Properties config = new Properties();
            InputStream in = null;
            try {
                in = Config.class.getClassLoader().getResourceAsStream(fileName);
            } catch (Exception e) {
            } finally {
                try {
                    config.load(in);
                    in.close();
                } catch (IOException ioe) {
                    logger.error(ioe.getMessage(),
                                 ioe);
                }
            }

        if(config.get("bootstrap.servers")== null){
            config.put("bootstrap.servers", getBotStrapServers());
        }
        return config;
    }

    private static void logConfig(Properties producerProperties) {
        if (logger.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Object, Object> entry : producerProperties.entrySet()) {
                sb.append(entry.getKey().toString()).append(":").append(entry.getValue()).append("  \n");
            }
            logger.info(sb.toString());
        }
    }
}
