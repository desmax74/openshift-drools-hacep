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
package org.kie.hacep.core.infra.consumer.loggable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.EventConsumer;
import org.kie.hacep.core.infra.consumer.ItemToProcess;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.ConsumerUtils;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.util.Printer;
import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.kie.remote.impl.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;

/**
 * The default consumer relies on the Consumer thread and
 * is based on the loop around poll method.
 */
public class DefaultKafkaConsumer<T> implements EventConsumer {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsEvents = new HashMap<>();
    private Consumer<String, T> kafkaConsumer, kafkaSecondaryConsumer;
    private ConsumerHandler consumerHandler;
    private volatile String processingKey = "";
    private volatile long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;
    private volatile boolean started, exit = false;
    private volatile State currentState = State.REPLICA;
    private volatile PolledTopic polledTopic = PolledTopic.CONTROL;
    private int iterationBetweenSnapshot;
    private List<ConsumerRecord<String, T>> eventsBuffer;
    private List<ConsumerRecord<String, T>> controlBuffer;
    private AtomicInteger counter;
    private SnapshotInfos snapshotInfos;
    private SessionSnapshooter snapShooter;
    private Printer printer;
    private EnvConfig envConfig;
    private Logger loggerForTest;
    private volatile boolean askedSnapshotOnDemand;

    public DefaultKafkaConsumer(EnvConfig config) {
        this.envConfig = config;
        if (this.envConfig.isSkipOnDemanSnapshot()) {
            counter = new AtomicInteger(0);
        }
        iterationBetweenSnapshot = this.envConfig.getIterationBetweenSnapshot();
        this.printer = PrinterUtil.getPrinter(this.envConfig);
        if (this.envConfig.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(this.envConfig);
        }
    }

    public void initConsumer(Producer producer) {
        this.consumerHandler = new DroolsConsumerHandler(producer,
                                                         envConfig);
        this.snapShooter = this.consumerHandler.getSnapshooter();
        this.kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        if (currentState.equals(State.REPLICA)) {
            this.kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }

    protected void restartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        assign();
        if (currentState.equals(State.REPLICA)) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        } else {
            kafkaSecondaryConsumer = null;
        }
    }

    @Override
    public void stop() {
        stopConsume();
        kafkaConsumer.wakeup();
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.wakeup();
        }
        exit = true;
        consumerHandler.stop();
    }

    @Override
    public void updateStatus(State state) {
        boolean changedState = !state.equals(currentState);
        if (currentState == null || changedState) {
            currentState = state;
        }
        if (started && changedState && !currentState.equals(State.BECOMING_LEADER)) {
            updateOnRunningConsumer(state);
        } else if (!started) {
            if (state.equals(State.REPLICA)) {
                //ask and wait a snapshot before start
                if (!envConfig.isSkipOnDemanSnapshot() && !askedSnapshotOnDemand) {
                    if (logger.isInfoEnabled()) {
                        logger.info("askAndProcessSnapshotOnDemand:");
                    }
                    askAndProcessSnapshotOnDemand();
                }
            }
            //State.BECOMING_LEADER won't start the pod
            if (state.equals(State.LEADER) || state.equals(State.REPLICA)) {
                if (logger.isInfoEnabled()) {
                    logger.info("enableConsumeAndStartLoop:{}",
                                state);
                }
                enableConsumeAndStartLoop(state);
            }
        }
    }

    public void askAndProcessSnapshotOnDemand() {
        askedSnapshotOnDemand = true;
        boolean completed = ((DroolsConsumerHandler) consumerHandler).initializeKieSessionFromSnapshotOnDemand(envConfig);
        if (logger.isInfoEnabled()) {
            logger.info("askAndProcessSnapshotOnDemand:{}",
                        completed);
        }
        if (!completed) {
            throw new RuntimeException("Can't obtain a snapshot on demand");
        }
    }

    protected void assign() {
        if (currentState.equals(State.LEADER)) {
            assignAsALeader();
        } else {
            assignReplica();
        }
    }

    protected void assignAsALeader() {
        assignConsumer(kafkaConsumer,
                       envConfig.getEventsTopicName());
    }

    protected void assignReplica() {
        assignConsumer(kafkaConsumer,
                       envConfig.getEventsTopicName());
        assignConsumer(kafkaSecondaryConsumer,
                       envConfig.getControlTopicName());
    }

    protected void assignConsumer(Consumer<String, T> kafkaConsumer,
                                  String topic) {

        List<PartitionInfo> partitionsInfo = kafkaConsumer.partitionsFor(topic);
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                partitionCollection.add(new TopicPartition(partition.topic(),
                                                           partition.partition()));
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
            if (currentState.equals(State.LEADER)) {
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedEventOffset));
            } else if (currentState.equals(State.REPLICA)) {
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedControlOffset));
            }
        }
    }

    public void poll() {

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
            while (!exit) {
                consume();
            }
        } catch (WakeupException e) {
            //nothind to do
        } finally {
            try {
                kafkaConsumer.commitSync();
                kafkaSecondaryConsumer.commitSync();
            } catch (WakeupException e) {
                //nothing to do
            } finally {
                logger.info("Closing kafkaConsumer on the loop");
                kafkaConsumer.close();
                kafkaSecondaryConsumer.close();
            }
        }
    }

    public void updateOnRunningConsumer(State state) {
        logger.info("updateOnRunning COnsumer");
        if (state.equals(State.LEADER)) {
            DroolsExecutor.setAsLeader();
            restart(state);
        } else if (state.equals(State.REPLICA)) {
            DroolsExecutor.setAsReplica();
            restart(state);
        }
    }

    public void restart(State state) {
        stopConsume();
        restartConsumer();
        enableConsumeAndStartLoop(state);
    }

    public void enableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER)) {
            currentState = State.LEADER;
            DroolsExecutor.setAsLeader();
        } else if (state.equals(State.REPLICA)) {
            currentState = State.REPLICA;
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
            DroolsExecutor.setAsReplica();
        }
        setLastProcessedKey();
        assignAndStartConsume();
    }

    public void setLastProcessedKey() {
        ControlMessage lastControlMessage = ConsumerUtils.getLastEvent(envConfig.getControlTopicName(),
                                                                       envConfig.getPollTimeout());
        settingsOnAEmptyControlTopic(lastControlMessage);
        processingKey = lastControlMessage.getId();
        processingKeyOffset = lastControlMessage.getOffset();
    }

    public void settingsOnAEmptyControlTopic(ControlMessage lastWrapper) {
        if (lastWrapper.getId() == null) {// completely empty or restart of ephemeral already used
            if (currentState.equals(State.REPLICA)) {
                pollControl();
            }
        }
    }

    public void assignAndStartConsume() {
        assign();
        startConsume();
    }

    public void consume() {
        if (started) {
            if (currentState.equals(State.LEADER)) {
                defaultProcessAsLeader();
            } else {
                defaultProcessAsAReplica();
            }
        }
    }

    protected void defaultProcessAsLeader() {
        pollEvents();
        if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
            consumeEventsFromBufferAsALeader();
        }
        ConsumerRecords<String, T> records = kafkaConsumer.poll(envConfig.getPollDuration());
        if (!records.isEmpty() && eventsBuffer == null) {
            ConsumerRecord<String, T> first = records.iterator().next();
            eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                              first.partition()));
            consumeEventsFromBufferAsALeader();
        } else {
            pollControl();
        }
    }

    protected void processLeader(ConsumerRecord<String, T> record) {

        if (envConfig.isSkipOnDemanSnapshot()) {
            handleSnapshotBetweenIteration(record);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record),
                                    currentState);
        }
        processingKey = record.key();// the new processed became the new processingKey
        saveOffset(record,
                   kafkaConsumer);

        if (logger.isInfoEnabled() || envConfig.isUnderTest()) {
            printer.prettyPrinter("DefaulImprovedKafkaConsumer.processLeader record:{}",
                                  record,
                                  true);
        }
    }

    protected void consumeEventsFromBufferAsALeader() {
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processLeader(record);
        }
        eventsBuffer = null;
    }

    protected void handleSnapshotBetweenIteration(ConsumerRecord<String, T> record) {
        int iteration = counter.incrementAndGet();
        if (iteration == iterationBetweenSnapshot) {
            counter.set(0);
            consumerHandler.processWithSnapshot(ItemToProcess.getItemToProcess(record),
                                                currentState);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record),
                                    currentState);
        }
    }

    protected void defaultProcessAsAReplica() {
        if (polledTopic.equals(PolledTopic.EVENTS)) {
            if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
                consumeEventsFromBufferAsAReplica();
            }
            ConsumerRecords<String, T> records = kafkaConsumer.poll(envConfig.getPollDuration());
            if (!records.isEmpty()) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                                  first.partition()));
                consumeEventsFromBufferAsAReplica();
            } else {
                pollControl();
            }
        }

        if (polledTopic.equals(PolledTopic.CONTROL)) {

            if (controlBuffer != null && controlBuffer.size() > 0) {
                consumeControlFromBufferAsAReplica();
            }

            ConsumerRecords<String, T> records = kafkaSecondaryConsumer.poll(envConfig.getPollDuration());
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                controlBuffer = records.records(new TopicPartition(first.topic(),
                                                                   first.partition()));
                consumeControlFromBufferAsAReplica();
            }
        }
    }

    protected void consumeEventsFromBufferAsAReplica() {
        if (envConfig.isUnderTest()) {
            loggerForTest.warn("consumeEventsFromBufferAsAReplica eventsBufferSize:{}",
                               eventsBuffer.size());
        }
        int index = 0;
        int end = eventsBuffer.size();
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processEventsAsAReplica(record);
            index++;
            if (polledTopic.equals(PolledTopic.CONTROL)) {
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

    protected void consumeControlFromBufferAsAReplica() {
        for (ConsumerRecord<String, T> record : controlBuffer) {
            processControlAsAReplica(record);
        }
        controlBuffer = null;
    }

    protected void processEventsAsAReplica(ConsumerRecord<String, T> record) {

        ItemToProcess item = ItemToProcess.getItemToProcess(record);
        if (record.key().equals(processingKey)) {
            lastProcessedEventOffset = record.offset();

            pollControl();

            if (logger.isDebugEnabled()) {
                logger.debug("processEventsAsAReplica change topic, switch to consume control, still {} events in the eventsBuffer to consume and processing item:{}.",
                             eventsBuffer.size(),
                             item);
            }
            consumerHandler.process(item,
                                    currentState);
            saveOffset(record,
                       kafkaConsumer);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("processEventsAsAReplica still {} events in the eventsBuffer to consume and processing item:{}.",
                             eventsBuffer.size(),
                             item);
            }
            consumerHandler.process(ItemToProcess.getItemToProcess(record),
                                    currentState);
            saveOffset(record,
                       kafkaConsumer);
        }
    }

    protected void processControlAsAReplica(ConsumerRecord<String, T> record) {

        if (record.offset() == processingKeyOffset + 1 || record.offset() == 0) {
            lastProcessedControlOffset = record.offset();
            processingKey = record.key();
            processingKeyOffset = record.offset();
            ControlMessage wr = deserialize((byte[]) record.value());
            consumerHandler.processSideEffectsOnReplica(wr.getSideEffects());

            pollEvents();
            if (logger.isDebugEnabled()) {
                logger.debug("change topic, switch to consume events");
            }
        }
        if (processingKey == null) { // empty topic
            processingKey = record.key();
            processingKeyOffset = record.offset();
        }
        saveOffset(record,
                   kafkaSecondaryConsumer);
    }

    protected void saveOffset(ConsumerRecord<String, T> record,
                              Consumer<String, T> kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(),
                                   record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        kafkaConsumer.commitSync(map);
    }

    public void startConsume() {
        started = true;
    }

    public void stopConsume() {
        started = false;
    }

    public void pollControl() {
        polledTopic = PolledTopic.CONTROL;
    }

    public void pollEvents() {
        polledTopic = PolledTopic.EVENTS;
    }

    public enum PolledTopic {
        EVENTS,
        CONTROL,
        NONE;
    }
}