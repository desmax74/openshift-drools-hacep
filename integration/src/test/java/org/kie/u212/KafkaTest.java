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
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.KafkaKieServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class KafkaTest {

    private  KafkaKieServerTest kafkaServerTest;
    private Logger kafkaLogger = LoggerFactory.getLogger("org.kie.u212.kafkaLogger");
    private final  String TEST_KAFKA_LOGGER_TOPIC = "testevents";
    private final  String TEST_TOPIC = "test";

    @Before
    public  void setUp() throws Exception{
        kafkaServerTest = new KafkaKieServerTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.createTopic(TEST_TOPIC);
    }

    @After
    public  void tearDown(){
        kafkaServerTest.deleteTopic(TEST_TOPIC);
        kafkaServerTest.deleteTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.shutdownServer();
    }


    @Test
    public void basicTest() throws IOException {
        kafkaServerTest = new KafkaKieServerTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopic(TEST_TOPIC);

        KafkaProducer<String, byte[]> producer = kafkaServerTest.getByteArrayProducer();
        KafkaConsumer<String, byte[]> consumer = kafkaServerTest.getByteArrayConsumer(TEST_TOPIC);

        ProducerRecord data = new ProducerRecord(TEST_TOPIC, "42", "test-message".getBytes(StandardCharsets.UTF_8));
        kafkaLogger.warn(data.toString());
        kafkaServerTest.sendSingleMsg(producer, data);

        // consume msg
        ConsumerRecords<String, byte[]> records = consumer.poll(5000);

        assertEquals(1, records.count());
        Iterator<ConsumerRecord<String, byte[]>> recordIterator = records.iterator();
        ConsumerRecord<String, byte[]> record = recordIterator.next();

        //assertions
        assertEquals("42", record.key());
        assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));

        kafkaServerTest.deleteTopic(TEST_TOPIC);
        kafkaServerTest.shutdownServer();
    }

    @Test
    public void testKafkaLoggerTest() {
        KafkaConsumer<String, String> consumerKafkaLogger = kafkaServerTest.getStringConsumer(TEST_KAFKA_LOGGER_TOPIC);
        kafkaLogger.warn("test-message");
        ConsumerRecords<String, String> records = consumerKafkaLogger.poll(5000);
        assertEquals(1, records.count());
        Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
        ConsumerRecord<String, String> record = recordIterator.next();
        assertNotNull(record);
        assertEquals(record.topic(), TEST_KAFKA_LOGGER_TOPIC);
        assertEquals(record.value(), "test-message\n");
    }


}
