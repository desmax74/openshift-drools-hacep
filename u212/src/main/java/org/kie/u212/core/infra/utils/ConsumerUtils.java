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
package org.kie.u212.core.infra.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerUtils<T> {

    private static Logger logger = LoggerFactory.getLogger(ConsumerUtils.class);

    public static void prettyPrinter(String id,
                                     String groupId,
                                     ConsumerRecord consumerRecord) {
        if (consumerRecord != null && logger.isInfoEnabled()) {
            logger.info("Id: {} - Group id {} - Topic: {} - Partition: {} - Offset: {} - Key: {} - Value: {}\n",
                        id,
                        groupId,
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        consumerRecord.key(),
                        consumerRecord.value());
        }
    }

    public static void printOffset(String topic,
                                   Properties configuration) {
        Map<TopicPartition, Long> offsets = getOffsets(topic,
                                                      configuration);
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            logger.info("Topic:{} offset:{}",
                        entry.getKey(),
                        entry.getValue());
        }
    }

    public static Map<TopicPartition, Long> getOffsets(String topic,
                                                      Properties configuration) {
        KafkaConsumer consumer = new KafkaConsumer(configuration);
        consumer.subscribe(Arrays.asList(topic));
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> tps = new ArrayList<>();
        for (PartitionInfo info : infos) {
            tps.add(new TopicPartition(topic,
                                       info.partition()));
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(tps);
        consumer.close();
        return offsets;
    }

    private static Long getOffset(String topic,
                                  Properties props) {
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> tps = new ArrayList<>();
        for (PartitionInfo info : infos) {
            tps.add(new TopicPartition(topic,
                                       info.partition()));
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(tps);
        Long lastOffset = 0l;
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            logger.info(entry.getKey() + ":" + entry.getValue());
            lastOffset = entry.getValue();
        }
        consumer.close();
        return lastOffset;
    }

    public static EventWrapper getLastEvent(String topic,
                                            Properties props) {
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();
        if (infos != null) {
            for (PartitionInfo partition : infos) {
                partitions.add(new TopicPartition(topic,
                                                  partition.partition()));
            }
        }
        consumer.assign(partitions);

        Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
        Long lastOffset = 0l;
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            lastOffset = entry.getValue();
        }
        logger.info("last offset:{}",
                    lastOffset);

        Set<TopicPartition> assignments = consumer.assignment();
        for (TopicPartition part : assignments) {
            consumer.seek(part,
                          lastOffset - 1);
        }

        EventWrapper eventWrapper = new EventWrapper();
        try {
            ConsumerRecords records = consumer.poll(1000);
            for (Object item : records) {
                ConsumerRecord<String, EventWrapper> record = (ConsumerRecord<String, EventWrapper>) item;
                eventWrapper.setEventType(EventType.APP);
                eventWrapper.setID(record.key());
                eventWrapper.setOffset(record.offset());
                Map map = (Map) record.value().getDomainEvent();
                StockTickEvent ticket = new StockTickEvent(map.get("company").toString(),
                                                           Double.valueOf(map.get("price").toString()));
                ticket.setTimestamp(record.timestamp());
                Date date = new Date(record.timestamp());
                logger.info("Date:{}", date);

                logger.info("Instant:{}",date.toInstant());
                eventWrapper.setDomainEvent(ticket);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(),
                         ex);
        } finally {
            consumer.close();
        }
        return eventWrapper;
    }

    public static EventWrapper getLastEvent(Long offset,
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
        assignments.forEach(topicPartition -> consumer.seek(topicPartition,
                                                            offset));
        EventWrapper eventWrapper = new EventWrapper();
        try {
            ConsumerRecords records = consumer.poll(1000);
            for (Object item : records) {
                ConsumerRecord<String, EventWrapper> record = (ConsumerRecord<String, EventWrapper>) item;
                eventWrapper.setEventType(EventType.APP);
                eventWrapper.setID(record.key());
                eventWrapper.setOffset(record.offset());
                Map map = (Map) record.value().getDomainEvent();
                StockTickEvent ticket = new StockTickEvent(map.get("company").toString(),
                                                           Double.valueOf(map.get("price").toString()));
                ticket.setTimestamp(record.timestamp());
                eventWrapper.setDomainEvent(ticket);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(),
                         ex);
        } finally {
            consumer.close();
        }
        return eventWrapper;
    }



}
