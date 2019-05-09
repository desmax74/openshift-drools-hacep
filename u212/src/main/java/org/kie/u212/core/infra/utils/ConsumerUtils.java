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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
import org.kie.u212.Config;
import org.kie.u212.ConverterUtil;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerUtils {

    private static Logger logger = LoggerFactory.getLogger(ConsumerUtils.class);

    public static void prettyPrinter(ConsumerRecord consumerRecord, boolean processed) {
        if (consumerRecord != null && logger.isInfoEnabled()) {
            logger.info("Processed:{} - Topic: {} - Partition: {} - Offset: {} - Value: {}\n",
                        processed,
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        consumerRecord.value());
        }
    }

    public static void printOffset(String topic) {
        Map<TopicPartition, Long> offsets = getOffsets(topic);
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            logger.info("Topic:{} offset:{}",
                        entry.getKey(),
                        entry.getValue());
        }
    }

    public static Map<TopicPartition, Long> getOffsets(String topic) {
        KafkaConsumer consumer = new KafkaConsumer(Config.getConsumerConfig());
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

    public static EventWrapper getLastEvent(String topic) {
        return getLastEvent(topic, Config.getConsumerConfig());
    }

    public static EventWrapper getLastEvent(String topic, Properties properties) {
        KafkaConsumer consumer = new KafkaConsumer(properties);
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();
        if (infos != null) {
            for (PartitionInfo partition : infos) {
                partitions.add(new TopicPartition(topic, partition.partition()));
            }
        }
        consumer.assign(partitions);

        Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
        Long lastOffset = 0l;
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            lastOffset = entry.getValue();
        }
        if(lastOffset == 0){
            lastOffset = 1l;// this is to start the seek with offset -1 on empty topic
        }
        Set<TopicPartition> assignments = consumer.assignment();
        for (TopicPartition part : assignments) {
            consumer.seek(part, lastOffset - 1);
        }

        EventWrapper eventWrapper = new EventWrapper();
        try {
            ConsumerRecords records = consumer.poll(Duration.of(Config.DEFAULT_POLL_TIMEOUT_MS, ChronoUnit.MILLIS));
            for (Object item : records) {
                ConsumerRecord<String, EventWrapper> record = (ConsumerRecord<String, EventWrapper>) item;
                eventWrapper.setEventType(EventType.APP);
                eventWrapper.setKey(record.key());
                eventWrapper.setOffset(record.offset());
                eventWrapper.setTimestamp(record.timestamp());
                if(record.value() != null) {
                    Map map = (Map) record.value().getDomainEvent();
                    StockTickEvent ticket = ConverterUtil.fromMap(map);
                    ticket.setTimestamp(record.timestamp());
                    Date date = new Date(record.timestamp());
                    logger.info("Timestamp Date last offset:{}", date);
                    eventWrapper.setDomainEvent(ticket);
                }else{
                    logger.info("no Event Wrapper");
                }
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
