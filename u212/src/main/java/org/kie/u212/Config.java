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
    public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST";
    public static final String BROKER_URL = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST);
    public static final int DEFAULT_POLL_SIZE = 1000;
    public static final int LOOP_DURATION = -1;
    public static final boolean DEFAULT_COMMIT_SYNC = true;
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    public static String getBotStrapServers() {
        StringBuilder sb = new StringBuilder();
        sb.append(Config.BROKER_URL).append(":9092");
        //append("my-cluster-kafka-bootstrap.my-kafka-project.svc:9092");//plain
        //.append(",").append("my-cluster-kafka-brokers.my-kafka-project.svc").append(":9093");//tls
        return sb.toString();
    }

    public static Properties getDefaultConfig() {
        Properties props = new Properties();
        props.put("key.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                  "org.kie.u212.consumer.EventJsonSerializer");
        props.put("bootstrap.servers",
                  getBotStrapServers());
        props.put("key.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                  "org.kie.u212.producer.EventJsonDeserializer");
        props.put("max.poll.interval.ms",
                  "10000");//time to discover the new consumer after a changetopic default 5 min 300000
        props.put("batch.size",
                  "16384");
        props.put("enable.auto.commit",
                  "false");
        props.put("metadata.max.age.ms",
                  "10000");
        props.setProperty("enable.auto.commit",
                          String.valueOf(true));
        //logConfig(props);
        return props;
    }

    public static Properties getDefaultConfigFromProps() {//@TODO
        Properties props = new Properties();

        InputStream in = null;
        try {
            in = Config.class.getClassLoader().getResourceAsStream("configuration.properties");
        } catch (Exception e) {
        } finally {
            try {
                props.load(in);
                in.close();
            } catch (IOException ioe) {
                logger.error(ioe.getMessage(),
                             ioe);
            }
        }
        return props;
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
