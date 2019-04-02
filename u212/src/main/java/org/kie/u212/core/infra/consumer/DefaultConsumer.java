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
package org.kie.u212.core.infra.consumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.kie.u212.Config;
import org.kie.u212.core.infra.OffsetManager;
import org.kie.u212.core.infra.PartitionListener;
import org.kie.u212.core.infra.election.Callback;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConsumer<T> implements EventConsumer,
                                           Callback {

    private Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private org.apache.kafka.clients.consumer.Consumer<String, T> kafkaConsumer;
    private ConsumerHandler consumerHandle;
    private String id;
    private String groupId;
    private boolean autoCommit;
    private boolean subscribeMode = false;
    private Restarter externalContainer;
    private volatile State currentState;
    private volatile boolean leader = false;
    private volatile boolean started = false;
    private volatile String startProcessingKey= "";
    private volatile boolean processing = true;
    private Properties configuration;


    public DefaultConsumer(String id,
                           Properties properties,
                           Restarter externalContainer) {
        this.id = id;
        this.configuration = properties;
        this.groupId = properties.getProperty("group.id");
        this.externalContainer = externalContainer;
    }


    public Consumer getKafkaConsumer() {
        return kafkaConsumer;
    }


    public void setKafkaConsumer(Consumer newConsumer) {
        this.kafkaConsumer = newConsumer;
    }


    public void setSubscribeMode(boolean subscribeMode) {
        this.subscribeMode = subscribeMode;
    }


    public void createConsumer(ConsumerHandler consumerHandler,
                      Properties properties) {
        this.consumerHandle = consumerHandler;
        kafkaConsumer = new KafkaConsumer<>(properties);
    }


    public void stop() {
        kafkaConsumer.close();
        started = false;
    }


    public void internalStart() {
        started = true;
    }


    public void waitStart(int pollSize,
                          long duration,
                          boolean commitSync) {
        while (true) {
            poll(pollSize,
                 duration,
                 commitSync);
        }
    }


    public void updateStatus(State state) {
        if (started) {
            updateOnRunningConsumer(state);
        } else {
            enableConsumeAndStartLoop(state);
        }
        currentState = state;
    }


    @Override
    public void subscribe(String groupId, String topic, boolean autoCommit) {
        this.autoCommit = autoCommit;
        this.groupId = groupId;
        kafkaConsumer.subscribe(Collections.singletonList(topic), new PartitionListener(kafkaConsumer, offsets));
    }

   /* @Override
    public boolean assign(String topic,
                          List partitions,
                          boolean autoCommit) {
        boolean isAssigned = false;
        List<PartitionInfo> partitionsInfo = kafkaConsumer.partitionsFor(topic);
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                if (partitions == null || partitions.contains(partition.partition())) {
                    partitionCollection.add(new TopicPartition(partition.topic(), partition.partition()));
                }
            }

            if (!partitionCollection.isEmpty()) {
                kafkaConsumer.assign(partitionCollection);
                isAssigned = true;
            }
        }
        this.autoCommit = autoCommit;
        return isAssigned;
    }*/

    @Override
    public boolean assign(String topic,
                          List partitions,
                          boolean autoCommit) {
        boolean isAssigned = false;
        List<PartitionInfo> partitionsInfo = kafkaConsumer.partitionsFor(topic);
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                if (partitions == null || partitions.contains(partition.partition())) {
                    partitionCollection.add(new TopicPartition(partition.topic(), partition.partition()));
                }
            }

            if (!partitionCollection.isEmpty()) {
                kafkaConsumer.assign(partitionCollection);
                isAssigned = true;
            }
        }
        Set<TopicPartition> assignments = kafkaConsumer.assignment();
        assignments.forEach(topicPartition -> kafkaConsumer.seekToBeginning(partitionCollection));
        this.autoCommit = autoCommit;
        return isAssigned;
    }


    @Override
    public void poll(int size,
                     long duration,
                     boolean commitSync) {

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Starting exit...\n");
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(),
                             e);
            }
        }));

        if (kafkaConsumer == null) {
            throw new IllegalStateException("Can't poll, kafkaConsumer not subscribed or null!");
        }

        try {
            if (duration == -1) {
                while (true) {
                    consume(size,
                            commitSync);
                }
            } else {
                long startTime = System.currentTimeMillis();
                while (false || (System.currentTimeMillis() - startTime) < duration) {
                    consume(size,
                            commitSync);
                }
            }
        } catch (WakeupException e) {
        } finally {
            try {
                kafkaConsumer.commitSync();
                if (logger.isDebugEnabled()) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        logger.debug("Consumer %s - partition %s - lastOffset %s\n",
                                     this.id,
                                     entry.getKey().partition(),
                                     entry.getValue().offset());
                    }
                }
                OffsetManager.store(offsets);
            } finally {
                logger.info("Closing kafkaConsumer on the loop");
                kafkaConsumer.close();
            }
        }
    }


    public void changeTopic(String newTopic) {
        started = false;
        externalContainer.changeTopic(newTopic, offsets);
        started = true;
    }





    private void updateOnRunningConsumer(State state) {
        if (state.equals(State.LEADER) && !leader) {
            leader = true;
            //changeTopic(Config.EVENTS_TOPIC);
        } else if (state.equals(State.NOT_LEADER) && leader) {
            leader = false;
            //changeTopic(Config.CONTROL_TOPIC);
        } else if (state.equals(State.NOT_LEADER) && !leader) {
            leader = false;
            //startConsume(Config.CONTROL_TOPIC);
        }
    }


    private void enableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER) && !leader) {
            leader = true;
            //startConsume(Config.EVENTS_TOPIC);
        } else if (state.equals(State.NOT_LEADER) && leader) {
            leader = false;
            //startConsume(Config.CONTROL_TOPIC);
        } else if (state.equals(State.NOT_LEADER) && !leader) {
            leader = false;
        }
        setLastprocessedKey();
        startConsume(Config.EVENTS_TOPIC);
    }


    private void setLastprocessedKey(){
        EventWrapper<StockTickEvent> lastWrapper = ConsumerUtils.getLastEvent(Config.EVENTS_TOPIC, configuration);
        startProcessingKey = lastWrapper.getID();
        logger.info("Last processedEvent ID:{} Timestamp:{}", lastWrapper.getID(), lastWrapper.getTimestamp());
    }


    private void startConsume(String topic) {
        if (subscribeMode) {
            subscribe(groupId, topic, autoCommit);
            started = true;
        } else {
            logger.info("assign");
            assign(topic, null, autoCommit);
            started = true;
        }
    }


    private void consume(int size,
                         boolean commitSync) {
        if (started) {
            defaultProcess(size, commitSync);
        }
    }


    private void defaultProcess(int size, boolean commitSync) {
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(size, ChronoUnit.MILLIS));
        for (ConsumerRecord<String, T> record : records) {
            //store next offset to commit
            if(record.key().equals(startProcessingKey)) {
                logger.info("Found last processed key, stopping the processing ");
                processing = false; // we have found the last processed events on the contorl topic
            }

            ConsumerUtils.prettyPrinter(id, groupId, record, processing);
            if(processing) {
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1,
                                                  "null"));
                consumerHandle.process(record, currentState, this);
            }
        }

        if (!autoCommit) {
            if (!commitSync) {
                try {
                    //async doesn't do a retry
                    kafkaConsumer.commitAsync((map, e) -> {
                        if (e != null) {
                            logger.error(e.getMessage(),
                                         e);
                        }
                    });
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage(),
                                 e);
                }
            } else {
                kafkaConsumer.commitSync();
            }
        }
    }
}