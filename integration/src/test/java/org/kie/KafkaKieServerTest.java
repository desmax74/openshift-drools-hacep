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
package org.kie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaKieServerTest<K,V> implements AutoCloseable{

    private static final String ZOOKEPER_HOST = "127.0.0.1";
    private static final String BROKER_HOST = "127.0.0.1";
    private static final String BROKER_PORT = "9092";

    private  KafkaServer kafkaServer;
    private ZkUtils zkUtils;
    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;
    private String tmpDir ;
    private final static Logger log = LoggerFactory.getLogger(KafkaKieServerTest.class);

    public  KafkaServer startServer() throws IOException{
        tmpDir = Files.createTempDirectory("kafka-").toAbsolutePath().toString();
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZOOKEPER_HOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", tmpDir);
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
        try {
            log.info("delete folder:{}", tmpDir);
            Files.walk(Paths.get(tmpDir)).
                    sorted(Comparator.reverseOrder()).
                    map(Path::toFile).
                    forEach(File::delete);
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        shutdownServer();
    }

    public void sendSingleMsg(KafkaProducer<K,V> producer, ProducerRecord<K,V> data) {
         producer.send(data);
        producer.close();
    }

    public void sendMsgs(KafkaProducer<K,V> producer, List<ProducerRecord<K, V>> items) {
        for(ProducerRecord<K,V> item: items){
            producer.send(item);
        }
        producer.close();
    }

    private Properties getConsumerConfig(){
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.put("auto.offset.reset", "earliest");
        return consumerProps;
    }

    private Properties getProducerConfig(){
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.put("auto.offset.reset", "earliest");
        return consumerProps;
    }



    public void createTopic(String topic) {
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    }

    public void deleteTopic(String topic){
        AdminUtils.deleteTopic(zkUtils, topic);
    }

    public KafkaConsumer<K,V> getStringConsumer(String topic) {
        // setup consumer
        Properties consumerProps = getConsumerConfig();
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public KafkaConsumer<K,V> getByteArrayConsumer(String topic) {
        // setup consumer
        Properties consumerProps = getConsumerConfig();
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public KafkaProducer<K,V> getByteArrayProducer() {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }

    public KafkaProducer<K,V> getStringProducer() {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(producerProps);
    }

}
