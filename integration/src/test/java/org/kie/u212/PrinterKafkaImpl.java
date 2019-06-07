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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.u212.core.infra.utils.Printer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrinterKafkaImpl implements Printer {

    private Logger kafkaLogger = LoggerFactory.getLogger("org.u212");

    @Override
    public void prettyPrinter(ConsumerRecord consumerRecord,
                              boolean processed) {
        if (consumerRecord != null && kafkaLogger.isInfoEnabled()) {
            kafkaLogger.info("Processed:{} - Topic: {} - Partition: {} - Offset: {} - Value: {}\n",
                        processed,
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                             !(consumerRecord.value() instanceof byte[]) ? consumerRecord.value() : "bytes[]");
        }

    }


    public void printOffset(String topic) {
        Map<TopicPartition, Long> offsets = getOffsets(topic);
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            kafkaLogger.info("Topic:{} offset:{}",
                        entry.getKey(),
                        entry.getValue());
        }
    }

    public Map<TopicPartition, Long> getOffsets(String topic) {
        KafkaConsumer consumer = new KafkaConsumer(Config.getConsumerConfig());
        consumer.subscribe(Arrays.asList(topic));
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> tps = new ArrayList<>();
        for (PartitionInfo info : infos) {
            tps.add(new TopicPartition(topic, info.partition()));
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(tps);
        consumer.close();
        return offsets;
    }
}
