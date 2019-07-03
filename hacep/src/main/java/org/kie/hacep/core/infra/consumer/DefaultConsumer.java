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
package org.kie.hacep.core.infra.consumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.kie.hacep.Config;
import org.kie.hacep.ConverterUtil;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.consumer.DroolsExecutor;
import org.kie.hacep.core.infra.OffsetManager;
import org.kie.hacep.core.infra.SessionSnapShooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.election.LeadershipCallback;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.ConsumerUtils;
import org.kie.hacep.core.infra.utils.Printer;
import org.kie.hacep.model.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default consumer relies on the Consumer thread and
 * is based on the loop around poll method.
 */
public class DefaultConsumer<T> implements EventConsumer,
                                           LeadershipCallback {

    private Logger logger = LoggerFactory.getLogger(DefaultConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsEvents = new HashMap<>();
    private org.apache.kafka.clients.consumer.Consumer<String, T> kafkaConsumer, kafkaSecondaryConsumer;
    private ConsumerHandler consumerHandle;
    private Restarter externalContainer;
    private volatile State currentState;
    private volatile String processingKey = "";
    private volatile long processingKeyOffset;
    private volatile boolean leader = false;
    private volatile boolean started = false;
    private volatile boolean processingLeader, processingNotLeader = false;
    private volatile boolean pollingEvents, pollingControl = true;
    private int iterationBetweenSnapshot;
    private List<ConsumerRecord<String, T>> eventsBuffer;
    private List<ConsumerRecord<String, T>> controlBuffer;
    private AtomicInteger counter = new AtomicInteger(0);
    private SnapshotInfos snapshotInfos;
    private Queue<Object> sideEffects;
    private SessionSnapShooter snapShooter;
    private Printer printer;
    private EnvConfig config;

    public DefaultConsumer(Restarter externalContainer, Printer printer, EnvConfig config) {
        this.config = config;
        this.externalContainer = externalContainer;
        iterationBetweenSnapshot = Integer.valueOf(Config.getDefaultConfig().getProperty(Config.ITERATION_BETWEEN_SNAPSHOT));
        this.printer = printer;
    }

    public void createConsumer(ConsumerHandler consumerHandler) {
        this.consumerHandle = consumerHandler;
        snapShooter = ((DroolsConsumerHandler)consumerHandle).getSnapshooter();
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        if (!leader) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }

    public void createConsumer(ConsumerHandler consumerHandler,
                               SnapshotInfos infos) {
        this.snapshotInfos = infos;
        this.consumerHandle = consumerHandler;
        snapShooter = ((DroolsConsumerHandler)consumerHandle).getSnapshooter();
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        if (!leader) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }

    public void restartConsumer() {
        logger.info("Restart Consumers");
        snapshotInfos = snapShooter.deserialize();
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        assign(null);
        if (!leader) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        } else {
            kafkaSecondaryConsumer = null;
        }
    }

    public void stop() {
        stopConsume();
        stopPollingControl();
        stopPollingEvents();
        kafkaConsumer.wakeup();
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.wakeup();
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
    public void assign(List partitions) {
        if (leader) {
            assignAsALeader(partitions);
        } else {
            assignNotLeader(partitions);
        }
    }

    private void assignAsALeader(List partitions) {
        assignConsumer(kafkaConsumer,
                       config.getEventsTopicName(),
                       partitions);
    }

    private void assignNotLeader(List partitions) {
        assignConsumer(kafkaConsumer,
                       config.getEventsTopicName(),
                       partitions);
        assignConsumer(kafkaSecondaryConsumer,
                       config.getControlTopicName(),
                       partitions);
    }

    private void assignConsumer(Consumer<String, T> kafkaConsumer,
                                String topic,
                                List partitions) {
        List<PartitionInfo> partitionsInfo = kafkaConsumer.partitionsFor(topic);
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                if (partitions == null || partitions.contains(partition.partition())) {
                    partitionCollection.add(new TopicPartition(partition.topic(),
                                                               partition.partition()));
                }
            }

            if (!partitionCollection.isEmpty()) {
                kafkaConsumer.assign(partitionCollection);
            }
        }

        if (snapshotInfos != null) {
            if (partitionCollection.size() > 1) {
                throw new RuntimeException("The system must run with only one partition per topic");
            }
            kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                    snapshotInfos.getOffsetDuringSnapshot()));
        } else {
            kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seekToBeginning(partitionCollection));
        }
    }

    @Override
    public void poll(int size,
                     long duration,
                     boolean commitSync) {

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Starting exit...\n");
            kafkaConsumer.wakeup();
            if (kafkaSecondaryConsumer != null) {
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
            while (true) {
                consume(size);
            }
        } catch (WakeupException e) {
            //nothind to do
        } finally {
            try {
                kafkaConsumer.commitSync();
                kafkaSecondaryConsumer.commitSync();
                if (logger.isDebugEnabled()) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsEvents.entrySet()) {
                        logger.debug("Consumer partition %s - lastOffset %s\n",
                                     entry.getKey().partition(),
                                     entry.getValue().offset());
                    }
                }
                OffsetManager.store(offsetsEvents);
            }catch (WakeupException e) {
               // logger.error(e.getMessage(), e);
            }finally
             {
                logger.info("Closing kafkaConsumer on the loop");
                kafkaConsumer.close();
                kafkaSecondaryConsumer.close();
            }
        }
    }

    private void updateOnRunningConsumer(State state) {
        if (state.equals(State.LEADER) && !leader) {
            DroolsExecutor.setAsMaster();
            restart(state);
        } else if (state.equals(State.REPLICA) && leader) {
            DroolsExecutor.setAsSlave();
            restart(state);
        }
    }

    private void restart(State state) {
        stopConsume();
        restartConsumer();
        enableConsumeAndStartLoop(state);
    }

    private void enableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER) && !leader) {
            leader = true;
            checkAndStopSecondaryConsumer();
            DroolsExecutor.setAsMaster();
            stopLeaderProcessing();// we starts to processing only when the last key readed on bootstrap is reached
        } else if (state.equals(State.REPLICA) && leader) {
            leader = false;
            checksAndStartSecondaryConsumer();
            DroolsExecutor.setAsSlave();
            startProcessingNotLeader();
            startPollingEvents();
            stopPollingControl();
        } else if (state.equals(State.REPLICA) && !leader) {
            leader = false;
            checksAndStartSecondaryConsumer();
            DroolsExecutor.setAsSlave();
            startProcessingNotLeader();
            stopPollingEvents();
            startPollingControl();
        } else if (state.equals(State.BECOMING_LEADER) && !leader) {
            leader = true;
            DroolsExecutor.setAsMaster();
            stopLeaderProcessing();
            checkAndStopSecondaryConsumer();
        }

        setLastProcessedKey();
        assignAndStartConsume();
    }

    private void checksAndStartSecondaryConsumer() {
        if(kafkaSecondaryConsumer == null) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }

    private void checkAndStopSecondaryConsumer() {
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.wakeup();
            kafkaSecondaryConsumer = null;
        }
    }

    private void setLastProcessedKey() {
        ControlMessage lastWrapper = ConsumerUtils.getLastEvent(config.getControlTopicName());
        settingsOnAEmptyControlTopic(lastWrapper);
        processingKey = lastWrapper.getKey();
        processingKeyOffset = lastWrapper.getOffset();
    }

    private void settingsOnAEmptyControlTopic( ControlMessage lastWrapper) {
        if (lastWrapper.getKey() == null) {// completely empty or restart of ephemeral already used
            if (leader) {
                startProcessingLeader();
            } else {
                stopProcessingNotLeader();
                stopPollingEvents();
                startPollingControl();// the nodes start to poll only the controlTopic until a event is available
            }
        }
    }

    private void assignAndStartConsume() {
        assign(null);
        startConsume();
    }

    private void consume(int size) {
        if (started) {
            if (leader) {
                defaultProcessAsLeader(size);
            } else {
                defaultProcessAsAReplica(size);
            }
        }
    }

    private void defaultProcessAsLeader(int size) {
        startPollingEvents();
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(size,
                                                                            ChronoUnit.MILLIS));
        for (ConsumerRecord<String, T> record : records) {
            processLeader(record,
                          counter);
        }
    }

    private void processLeader(ConsumerRecord<String, T> record,
                               AtomicInteger counter) {
        printer.prettyPrinter(record,
                                    processingLeader);
        if (record.key().equals(processingKey)) {
            startProcessingLeader();
        } else if (processingLeader) {
            int iteration = counter.incrementAndGet();
            if (iteration == iterationBetweenSnapshot) {
                counter.set(0);
                consumerHandle.processWithSnapshot(ItemToProcess.getItemToProcess(record),
                                                   currentState,
                                                   this,
                                                   sideEffects);
            } else {
                consumerHandle.process(ItemToProcess.getItemToProcess(record),
                                       currentState,
                                       this,
                                       sideEffects);
            }
            processingKey = record.key();// the new processed became the new processingKey
            saveOffset(record,
                       kafkaConsumer);
        }
    }

    private void defaultProcessAsAReplica(int size) {

        if (pollingEvents) {
            if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
                consumeEventsFromBufferAsAReplica();
            }
            ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(size,
                                                                                ChronoUnit.MILLIS));
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                                  first.partition()));
                consumeEventsFromBufferAsAReplica();
            }
        }

        if (pollingControl) {

            if (controlBuffer != null && controlBuffer.size() > 0) {
                consumeControlFromBuffer();
            }

            ConsumerRecords<String, T> records = kafkaSecondaryConsumer.poll(Duration.of(size,
                                                                                         ChronoUnit.MILLIS));
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                controlBuffer = records.records(new TopicPartition(first.topic(),
                                                                   first.partition()));
                consumeControlFromBuffer();
            }
        }
    }

    private void consumeEventsFromBufferAsAReplica() {
        int index = 0;
        int end = eventsBuffer.size();
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processEventsAsANonLeader(record);
            index++;
            if (!pollingEvents) {
                if (end > index) {
                    eventsBuffer = eventsBuffer.subList(index,
                                                        end);
                }
                break;
            }
        }
        if (end == index) {
            eventsBuffer = null;
        }
    }

    private void consumeControlFromBuffer() {
        int index = 0;
        int end = controlBuffer.size();
        for (ConsumerRecord<String, T> record : controlBuffer) {
            processControlAsANonLeader(record);
            index++;
            if (!pollingControl) {
                if (end > index) {
                    controlBuffer = controlBuffer.subList(index,
                                                          end);
                }
                break;
            }
        }
        if (end == index) {
            controlBuffer = null;
        }
    }

    private void processEventsAsANonLeader(ConsumerRecord<String, T> record) {
        printer.prettyPrinter(record, processingNotLeader);
        if (record.key().equals(processingKey)) {
            stopPollingEvents();
            startPollingControl();
            stopProcessingNotLeader();
            consumerHandle.process(ItemToProcess.getItemToProcess(record),
                                   currentState,
                                   this,
                                   sideEffects);
            saveOffset(record, kafkaConsumer);
            logger.info("change topic, switch to consume control");
        } else if (processingNotLeader) {
            consumerHandle.process(ItemToProcess.getItemToProcess(record),
                                   currentState,
                                   this,
                                   sideEffects);
            saveOffset(record, kafkaConsumer);
        }
    }

    private void processControlAsANonLeader(ConsumerRecord<String, T> record) {
        printer.prettyPrinter(record,
                                    false);
        if (record.offset() == processingKeyOffset + 1 || record.offset() == 0) {
            if (record.offset() > 0) {
                processingKey = record.key();
                processingKeyOffset = record.offset();
                ControlMessage wr = ConverterUtil.deSerializeObjInto((byte[])record.value(), ControlMessage.class);
                sideEffects = wr.getSideEffects();
            }

            stopPollingControl();
            startPollingEvents();
            startProcessingNotLeader();
            logger.info("change topic, switch to consume events");
        }
        if (processingKey == null) { // empty topic
            processingKey = record.key();
            processingKeyOffset = record.offset();
        }
        saveOffset(record,
                   kafkaSecondaryConsumer);
    }

    private void saveOffset(ConsumerRecord<String, T> record,
                            Consumer<String, T> kafkaSecondaryConsumer) {
        Map map = new HashMap();
        map.put(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        kafkaSecondaryConsumer.commitSync(map);
    }

    private void stopProcessingNotLeader() {
        processingNotLeader = false;
    }

    private void startProcessingLeader() {
        processingLeader = true;// the leader starts to process from events topic and publish on control topic
    }

    private void startPollingControl() {
        pollingControl = true;
    }

    private void stopPollingControl() {
        pollingControl = false;
    }

    private void startConsume() {
        started = true;
    }

    private void stopConsume() {
        started = false;
    }

    private void startProcessingNotLeader() {
        processingNotLeader = true;
    }

    private void stopPollingEvents() {
        pollingEvents = false;
    }

    private void stopLeaderProcessing() {
        processingLeader = false;
    }

    private void startPollingEvents() {
        pollingEvents = true;
    }
}