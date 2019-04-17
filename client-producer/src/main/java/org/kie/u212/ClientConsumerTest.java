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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConsumerTest {

    private static Logger logger = LoggerFactory.getLogger(ClientConsumerTest.class);

    public static void main(String[] args) {
        Properties props = ClientUtils.getConfiguration(ClientUtils.CONSUMER_CONF);
        EventWrapper wrapper = ConsumerUtils.getLastEvent(Config.CONTROL_TOPIC, ClientUtils.getConfiguration(ClientUtils.CONSUMER_CONF));
        processAllEventsFromBegin(wrapper.getKey(), Config.CONTROL_TOPIC, props);
    }


    public static void processAllEventsFromBegin(String key, String topic, Properties props) {
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList();
        if (infos != null) {
            for (PartitionInfo partition : infos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
        }
        consumer.assign(partitions);
        Set<TopicPartition> assignments = consumer.assignment();
        assignments.forEach(topicPartition -> consumer.seekToBeginning(assignments));

        try {
            while(true) {
                ConsumerRecords records = consumer.poll(Duration.of(Config.DEFAULT_POLL_TIMEOUT_MS, ChronoUnit.MILLIS));
                records.forEach(record -> skipOrProcess(key, (ConsumerRecord<String, EventWrapper>)record));
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            consumer.close();
        }
    }

    public static void skipOrProcess(String key, ConsumerRecord<String, EventWrapper> record){
        if(record.key().equals(key)) {
            Map map = (Map) record.value().getDomainEvent();
            StockTickEvent ticket = new StockTickEvent(map.get("company").toString(),
                                                       Double.valueOf(map.get("price").toString()));
            ticket.setTimestamp(record.timestamp());
            logger.info(" key:{} offset:{} ticket:{}",
                        record.key(),
                        record.offset(),
                        ticket);
            return;
        }else {
            Map map = (Map) record.value().getDomainEvent();
            StockTickEvent ticket = new StockTickEvent(map.get("company").toString(),
                                                       Double.valueOf(map.get("price").toString()));
            ticket.setTimestamp(record.timestamp());
            logger.info(" key:{} offset:{} ticket:{}",
                        record.key(),
                        record.offset(),
                        ticket);
        }
    }
}
