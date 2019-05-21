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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;
import org.kie.test.kafka.KafkaKieServerTest;

import static org.junit.Assert.*;

public class KafkaTest {

    private KafkaKieServerTest kafkaServerTest;


    @Test
    public void producerTest() throws IOException {
        kafkaServerTest = new KafkaKieServerTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopic("test");

        KafkaProducer<Integer, byte[]> producer = kafkaServerTest.getProducer();
        KafkaConsumer<Integer, byte[]> consumer = kafkaServerTest.getConsumer();
        kafkaServerTest.sendMsg(producer);

        // consume msg
        ConsumerRecords<Integer, byte[]> records = consumer.poll(5000);

        assertEquals(1, records.count());
        Iterator<ConsumerRecord<Integer, byte[]>> recordIterator = records.iterator();
        ConsumerRecord<Integer, byte[]> record = recordIterator.next();

        System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());

        //assertions
        assertEquals(42, (int) record.key());
        assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));

        kafkaServerTest.shutdownServer();
    }


}
