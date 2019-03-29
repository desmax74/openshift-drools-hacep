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
package org.kie.u212.core.infra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.u212.Config;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * IS an hook to save information when the partition is moved
 * @param <T>
 */
public class PartitionListener<T> implements ConsumerRebalanceListener {

    private Logger logger = LoggerFactory.getLogger(PartitionListener.class);
    private Consumer<String, T> consumer;
    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public PartitionListener(Consumer<String, T> consumer,
                             Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        //TODO save inside the cluster, infinispan
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        notContainerEnvironment(partitions);
    }


    private void notContainerEnvironment(Collection<TopicPartition> partitions) {
        Properties properties = OffsetManager.load();
        //seek from offset
        for (TopicPartition partition : partitions) {
            try {
                String offset = properties.getProperty(partition.topic() + "-" + partition.partition());
                if (offset != null) {
                    consumer.seek(partition,
                                  Long.valueOf(offset));
                    logger.info("Consumer - partition {} - initOffset {}\n",
                                partition.partition(),
                                offset);
                }
            } catch (Exception ex) {
                logger.info("Consumer - partition {} - initOffset not from DB\n",
                            partition.partition());
            }
        }
    }
}
