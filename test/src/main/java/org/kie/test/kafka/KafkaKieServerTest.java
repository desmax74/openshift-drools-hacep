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
package org.kie.test.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaKieServerTest {

    private static final String ZOOKEPER_HOST = "127.0.0.1";
    private static final String BROKER_HOST = "127.0.0.1";
    private static final String BROKER_PORT = "9092";
    private static final String TOPIC = "test";

    private  KafkaServer kafkaServer;
    private ZkUtils zkUtils;
    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;


    public  KafkaServer startServer() throws IOException{
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZOOKEPER_HOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKER_HOST +":" + BROKER_PORT);
        brokerProps.setProperty("offsets.topic.replication.factor" , "1");
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new SystemTime();
        kafkaServer = TestUtils.createServer(config, mock);
        return kafkaServer;
    }

    public void shutdownServer(){
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    public void sendMsg(KafkaProducer<Integer, byte[]> producer) {
        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC, 42, "test-message".getBytes(StandardCharsets.UTF_8));
        producer.send(data);
        producer.close();
    }

    public void createTopic(String topic) {
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    }

    public KafkaConsumer<Integer, byte[]> getConsumer() {
        // setup consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        return consumer;
    }

    public KafkaProducer<Integer, byte[]> getProducer() {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<Integer, byte[]>(producerProps);
    }
}
