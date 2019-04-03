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
import java.util.Date;
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
import org.kie.u212.ConverterUtil;
import org.kie.u212.core.infra.OffsetManager;

import org.kie.u212.core.infra.election.Callback;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConsumer<T> implements EventConsumer, Callback {

    private Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private org.apache.kafka.clients.consumer.Consumer<String, T> kafkaConsumer, kafkaSecondaryConsumer;
    private ConsumerHandler consumerHandle;
    private String id;
    private String groupId;
    private boolean autoCommit;
    private Restarter externalContainer;
    private volatile State currentState;
    private volatile boolean leader = false;
    private volatile boolean started = false;
    private volatile String startProcessingKey= "";
    private volatile boolean processing = false;
    private Properties configuration;


    public DefaultConsumer(String id,
                           Properties properties,
                           Restarter externalContainer) {
        this.id = id;
        this.configuration = properties;
        this.groupId = properties.getProperty("group.id");
        this.externalContainer = externalContainer;
    }


    public void createConsumer(ConsumerHandler consumerHandler,
                      Properties properties) {
        this.consumerHandle = consumerHandler;
        kafkaConsumer = new KafkaConsumer<>(properties);
        if(!leader) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(properties);
        }
    }


    public void stop() {
        kafkaConsumer.close();
        if(kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.close();
        }
        started = false;
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
    public void assign(List partitions, boolean autoCommit) {
        if(leader){
            assignAsALeader(partitions, autoCommit);
        }else{
            //@TODO
            assignNotLeader(partitions, autoCommit);
        }
    }

    private void assignAsALeader(List partitions, boolean autoCommit) {
        //Primary consumer
        assignConsumer(kafkaConsumer,Config.EVENTS_TOPIC, partitions);
        this.autoCommit = autoCommit;
    }

    private void assignNotLeader(List partitions, boolean autoCommit) {
        //Primary consumer
        assignConsumer(kafkaConsumer,Config.CONTROL_TOPIC, partitions);
        this.autoCommit = autoCommit;

        //SecondaryConsumer
        assignConsumer(kafkaSecondaryConsumer, Config.EVENTS_TOPIC,partitions);
    }

    private void assignConsumer(Consumer<String, T> kafkaConsumer, String topic, List partitions) {
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
            }
        }
        kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seekToBeginning(partitionCollection));
    }

    @Override
    public void poll(int size,
                     long duration,
                     boolean commitSync) {
        logger.info("poll");
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Starting exit...\n");
            kafkaConsumer.wakeup();
            if(kafkaSecondaryConsumer != null){
                kafkaSecondaryConsumer.wakeup();
            }
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

        if (kafkaSecondaryConsumer == null) {
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
                kafkaSecondaryConsumer.close();
            }
        }
    }


    private void updateOnRunningConsumer(State state) {
        //@TODO test if becoming leader is possible
        if (state.equals(State.LEADER) && !leader) {
            leader = true;
        } else if (state.equals(State.NOT_LEADER) && leader) {
            leader = false;
        }
    }


    private void enableConsumeAndStartLoop(State state) {
        logger.info("enableConsumeAndStartLoop");
        if (state.equals(State.LEADER) && !leader) {
            leader = true;
            processing = false;
        } else if (state.equals(State.NOT_LEADER) && leader) {
            leader = false;
            processing = true;
        } else if (state.equals(State.NOT_LEADER) && !leader) {
            leader = false;
            processing = true;
        } else if (state.equals(State.BECOMING_LEADER) && !leader) {
            leader = true;
            processing = false;
        }
        setLastprocessedKey();
        startConsume();

    }


    private void setLastprocessedKey(){
        EventWrapper<StockTickEvent> lastWrapper = ConsumerUtils.getLastEvent(Config.CONTROL_TOPIC, configuration);
        if(lastWrapper.getID() == null && lastWrapper.getOffset() == 0l){
            logger.info("Empty topic control");
            if(leader){
                processing = true;// the leader starts to process from events topic and publish on control topic
            }
        }
        startProcessingKey = lastWrapper.getID();
        logger.info("Last processedEvent on Control topic ID:{} Timestamp:{} Offset:{}", lastWrapper.getID(), lastWrapper.getTimestamp(), lastWrapper.getOffset());
    }


    private void startConsume() {
        assign(null, autoCommit);
        started = true;
    }


    private void consume(int size, boolean commitSync) {
        if (started) {
            if(leader) {
                defaultProcessAsLeader(size, commitSync);
            }else{
                defaultProcessAsNotLeader(size, commitSync);
            }
        }
    }


    private void defaultProcessAsLeader(int size, boolean commitSync) {
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(size, ChronoUnit.MILLIS));
        for (ConsumerRecord<String, T> record : records) {
            processLeader(record);
        }

        manageAutocommit(commitSync);
    }



    private void defaultProcessAsNotLeader(int size, boolean commitSync) {
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(size, ChronoUnit.MILLIS));
        for (ConsumerRecord<String, T> record : records) {
            processNonLeader(record);
        }

        manageAutocommit(commitSync);
    }

    private void manageAutocommit(boolean commitSync) {
        if (!autoCommit) {
            if (!commitSync) {
                try {
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

    private void processLeader(ConsumerRecord<String,T> record) {
        ConsumerUtils.prettyPrinter(id, groupId, record, processing);
        if(record.key().equals(startProcessingKey)) {
            logger.info("Reached last processed key, starting processing new events");
            processing = true;
        }else if(processing) {
            offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "null"));
            consumerHandle.process(record, currentState, this);
        }
    }

    private void processNonLeader(ConsumerRecord<String, T> record) {

        ConsumerUtils.prettyPrinter(id, groupId, record, processing);
        if(record.key().equals(startProcessingKey)) {
            logger.info("Reached last processed key, stopping the processing on events");
            processing = false;
            logger.info("Poll on Event Topic");
            started = false;//this put in pause the loop on control
            Set<TopicPartition> assignments = kafkaSecondaryConsumer.assignment();
            for (TopicPartition part : assignments) {
                kafkaSecondaryConsumer.seek(part, record.offset());
            }

        }else if (processing) {
            logger.info("processing");
            offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "null"));
            consumerHandle.process(record, currentState, this);
        }
    }

    private EventWrapper getNextControlEvent(){
        EventWrapper eventWrapper = new EventWrapper();

            ConsumerRecords records = kafkaSecondaryConsumer.poll(Duration.of(Config.DEFAULT_POLL_TIMEOUT_MS, ChronoUnit.MILLIS));
            for (Object item : records) {
                ConsumerRecord<String, EventWrapper> record = (ConsumerRecord<String, EventWrapper>) item;
                eventWrapper.setEventType(EventType.APP);
                eventWrapper.setID(record.key());
                eventWrapper.setOffset(record.offset());
                eventWrapper.setTimestamp(record.timestamp());
                Map map = (Map) record.value().getDomainEvent();
                StockTickEvent ticket = ConverterUtil.fromMap(map);
                ticket.setTimestamp(record.timestamp());
                Date date = new Date(record.timestamp());
                logger.info("Timestamp Date last offset:{}", date);
                eventWrapper.setDomainEvent(ticket);
            }
        return eventWrapper;
    }
}