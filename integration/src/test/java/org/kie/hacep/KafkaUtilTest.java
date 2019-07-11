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
package org.kie.hacep;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.Config;
import org.kie.remote.EnvConfig;
import org.kie.remote.RemoteCepKieSession;
import org.kie.remote.RemoteKieSession;
import org.kie.remote.impl.producer.RemoteCepKieSessionImpl;
import org.kie.remote.impl.producer.RemoteKieSessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtilTest implements AutoCloseable {

    private static final String ZOOKEPER_HOST = "127.0.0.1";
    private static final String BROKER_HOST = "127.0.0.1";
    private static final String BROKER_PORT = "9092";
    private final static Logger log = LoggerFactory.getLogger(KafkaUtilTest.class);
    private KafkaServer kafkaServer;
    private ZkUtils zkUtils;
    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;
    private String tmpDir;

    public KafkaServer startServer() throws IOException {
        tmpDir = Files.createTempDirectory("kafkatest-").toAbsolutePath().toString();
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZOOKEPER_HOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect,
                                30000,
                                30000,
                                ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient,
                                false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect",
                                zkConnect);
        brokerProps.setProperty("broker.id",
                                "0");
        brokerProps.setProperty("log.dirs",
                                tmpDir);
        brokerProps.setProperty("listeners",
                                "PLAINTEXT://" + BROKER_HOST + ":" + BROKER_PORT);
        brokerProps.setProperty("offsets.topic.replication.factor",
                                "1");
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new SystemTime();
        kafkaServer = TestUtils.createServer(config,
                                             mock);
        return kafkaServer;
    }

    public void shutdownServer() {
        log.info("Shutdown kafka server");
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
        Path tmp = Paths.get(tmpDir);
        try {
            Files.walk(tmp).
                    sorted(Comparator.reverseOrder()).
                    map(Path::toFile).
                    forEach(File::delete);
        } catch (Exception e) {
            log.error(e.getMessage(),
                      e);
        }
        //clean previous kafkatest- dirs
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(tmp.getParent())) {
            for (Path path : directoryStream) {
                if (path.toString().startsWith("kafkatest-")) {
                    Files.walk(path).
                            sorted(Comparator.reverseOrder()).
                            map(Path::toFile).
                            forEach(File::delete);
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(),
                      e);
        }
    }

    @Override
    public void close() {
        shutdownServer();
    }

    public <K, V> void sendSingleMsg(KafkaProducer<K, V> producer,
                                     ProducerRecord<K, V> data) {
        producer.send(data);
        producer.close();
    }


    private Properties getConsumerConfig() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers",
                                  BROKER_HOST + ":" + BROKER_PORT);
        consumerProps.setProperty("group.id",
                                  "group0");
        consumerProps.setProperty("client.id",
                                  "consumer0");
        consumerProps.put("auto.offset.reset",
                          "earliest");
        consumerProps.setProperty("key.deserializer",
                                  "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerProps;
    }

    private Properties getProducerConfig() {
        Properties producerProps = new Properties();
        producerProps.setProperty("key.serializer",
                                  "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("bootstrap.servers",
                                  BROKER_HOST + ":" + BROKER_PORT);
        return producerProps;
    }

    public void createTopic(String topic) {
        AdminUtils.createTopic(zkUtils,
                               topic,
                               1,
                               1,
                               new Properties(),
                               RackAwareMode.Disabled$.MODULE$);
    }

    public void deleteTopic(String topic) {
        AdminUtils.deleteTopic(zkUtils,
                               topic);
    }

    public <K, V> KafkaConsumer<K, V> getStringConsumer(String topic) {
        Properties consumerProps = getConsumerConfig();
        consumerProps.setProperty("value.deserializer",
                                  "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public <K, V> KafkaConsumer<K, V> getByteArrayConsumer(String topic) {
        Properties consumerProps = getConsumerConfig();
        consumerProps.setProperty("value.deserializer",
                                  "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public <K, V> KafkaProducer<K, V> getByteArrayProducer() {
        Properties producerProps = getProducerConfig();
        producerProps.setProperty("value.serializer",
                                  "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerProps);
    }

    public <K, V> KafkaProducer<K, V> getStringProducer() {
        Properties producerProps = getProducerConfig();
        producerProps.setProperty("value.serializer",
                                  "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(producerProps);
    }

    public KafkaConsumer getConsumer(String key,
                                     String topic,
                                     Properties props) {
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList();
        if (infos != null) {
            for (PartitionInfo partition : infos) {
                partitions.add(new TopicPartition(partition.topic(),
                                                  partition.partition()));
            }
        }
        consumer.assign(partitions);
        Set<TopicPartition> assignments = consumer.assignment();
        assignments.forEach(topicPartition -> consumer.seekToBeginning(assignments));
        return consumer;
    }

    public void insertBatchStockTicketEvent(int items,
                                            EnvConfig envConfig,
                                            Class sessionType) {
        Properties props = Config.getProducerConfig("InsertBactchStockTickets");
        if (sessionType.equals(RemoteKieSession.class)) {
            try (RemoteKieSessionImpl producer = new RemoteKieSessionImpl(props,
                                                                          envConfig)) {
                for (int i = 0; i < items; i++) {
                    StockTickEvent ticket = new StockTickEvent("RHT",
                                                               ThreadLocalRandom.current().nextLong(80,
                                                                                                    100));
                    producer.insert(ticket);
                }
            }
        }
        if (sessionType.equals(RemoteCepKieSession.class)) {
            try (RemoteCepKieSessionImpl producer = new RemoteCepKieSessionImpl(props,
                                                                                envConfig)) {
                for (int i = 0; i < items; i++) {
                    StockTickEvent ticket = new StockTickEvent("RHT",
                                                               ThreadLocalRandom.current().nextLong(80,
                                                                                                    100));
                    producer.insert(ticket);
                }
            }
        }
    }
}
