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
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.ConsumerUtils;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.util.Printer;
import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;

public class KafkaConsumers<T> implements Consumers<T> {

    private Logger logger = LoggerFactory.getLogger(KafkaConsumers.class);
    private Consumer<String, T> primaryConsumer, secondaryConsumer;
    private EnvConfig envConfig;
    private SnapshotInfos snapshotInfos;
    private SessionSnapshooter snapShooter;
    private EventConsumerStatus status;
    private List<ConsumerRecord<String,T>> eventsBuffer;
    private List<ConsumerRecord<String,T>> controlBuffer;
    private ConsumerHandler consumerHandler;
    private AtomicInteger counter ;
    private Printer printer;

    public KafkaConsumers(EventConsumerStatus status, EnvConfig envConfig, ConsumerHandler consumerHandler, SessionSnapshooter snapshooter){
        this.consumerHandler = consumerHandler;
        this.envConfig = envConfig;
        this.snapShooter = snapshooter;
        this.status = status;
        if(this.envConfig.isSkipOnDemanSnapshot()){
            counter = new AtomicInteger(0);
        }
        this.printer = PrinterUtil.getPrinter(this.envConfig);
    }

    @Override
    public void initConsumer() {
        this.primaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));;
        if (status.getCurrentState().equals(State.REPLICA)) {
            this.secondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }
    
    @Override
    public void stop() {
        primaryConsumer.wakeup();
        if (secondaryConsumer != null) {
            secondaryConsumer.wakeup();
        }
    }

    @Override
    public void poll() {
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Starting exit...\n");
            primaryConsumer.wakeup();
            if (secondaryConsumer != null) {
                secondaryConsumer.wakeup();
            }
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }));

        if (primaryConsumer == null) {
            throw new IllegalStateException("Can't poll, kafkaConsumer not subscribed or null!");
        }

        if (secondaryConsumer == null) {
            throw new IllegalStateException("Can't poll, kafkaSecondaryConsumer not subscribed or null!");
        }

        try {
            while (!status.isExit()) {
                consume();
            }
        } catch (WakeupException e) {
            //nothind to do
        } finally {
            try {
                primaryConsumer.commitSync();
                secondaryConsumer.commitSync();
            } catch (WakeupException e) {
                //nothing to do
            } finally {
                logger.info("Closing kafkaConsumer on the loop");
                primaryConsumer.close();
                secondaryConsumer.close();
            }
        }
    }

    private  void consume() {
        if (status.isStarted()) {
            if (status.getCurrentState().equals(State.LEADER)) {
                defaultProcessAsLeader();
            } else {
                defaultProcessAsAReplica();
            }
        }
    }

    @Override
    public void restartConsumer() {
        if (logger.isDebugEnabled()) {
            logger.debug("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?
        primaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));

        assign();
        if (status.getCurrentState().equals(State.REPLICA)) {
            secondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        } else {
            secondaryConsumer = null;
        }
    }

    private  void assign() {
        if (status.getCurrentState().equals(State.LEADER)) {
            assignAsALeader();
        } else {
            assignReplica();
        }
    }

    private  void assignAsALeader() {
        assignConsumer(primaryConsumer, envConfig.getEventsTopicName());
    }

    private void assignReplica() {
        assignConsumer(primaryConsumer, envConfig.getEventsTopicName());
        assignConsumer(secondaryConsumer, envConfig.getControlTopicName());
    }

    private void assignConsumer(Consumer kafkaConsumer, String topic) {

        List<PartitionInfo> partitionsInfo = kafkaConsumer.partitionsFor(topic);
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                partitionCollection.add(new TopicPartition(partition.topic(), partition.partition()));
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
            if(status.getCurrentState().equals(State.LEADER)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        status.getLastProcessedEventOffset()));
            }else if(status.getCurrentState().equals(State.REPLICA)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        status.getLastProcessedControlOffset()));
            }
        }
    }

    @Override
    public  void enableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER)) {
            status.setCurrentState(State.LEADER);
            DroolsExecutor.setAsLeader();

        } else if (state.equals(State.REPLICA) ) {
            status.setCurrentState(State.REPLICA);
            secondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
            DroolsExecutor.setAsReplica();
        }
        setLastProcessedKey();
        assignAndStartConsume();
    }

    @Override
    public  void setLastProcessedKey() {
        ControlMessage lastControlMessage = ConsumerUtils.getLastEvent(envConfig.getControlTopicName(), envConfig.getPollTimeout());
        settingsOnAEmptyControlTopic(lastControlMessage);
        status.setProcessingKey(lastControlMessage.getId());
        status.setProcessingKeyOffset(lastControlMessage.getOffset());
    }

    @Override
    public  void settingsOnAEmptyControlTopic(ControlMessage lastWrapper) {
        if (lastWrapper.getId() == null) {// completely empty or restart of ephemeral already used
            if (status.getCurrentState().equals(State.REPLICA)) {
                pollControl();
            }
        }
    }

    @Override
    public void internalRestartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?
        restartConsumer();
    }

    @Override
    public  void restart(State state) {
        stopConsume();
        internalRestartConsumer();
        enableConsumeAndStartLoop(state);
    }

    private  void assignAndStartConsume() {
        assign();
        startConsume();
    }

    private  void defaultProcessAsLeader() {
        pollEvents();
        ConsumerRecords<String, T> records = primaryConsumer.poll(envConfig.getPollDuration());
        if (!records.isEmpty()) {
            ConsumerRecord<String, T> first = records.iterator().next();
            eventsBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
            consumeEventsFromBufferAsALeader();
        } else {
            pollControl();
        }
    }

    private void defaultProcessAsAReplica() {
        if (status.getPolledTopic().equals(DefaultEventConsumerStatus.PolledTopic.EVENTS)) {
            ConsumerRecords<String, T> records = primaryConsumer.poll(envConfig.getPollDuration());
            if (!records.isEmpty()) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
                consumeEventsFromBufferAsAReplica();
            } else {
                pollControl();
            }
        }

        if (status.getPolledTopic().equals(DefaultEventConsumerStatus.PolledTopic.CONTROL)) {
            if (controlBuffer != null && controlBuffer.size() > 0) {
                consumeControlFromBufferAsAReplica();
            }
            ConsumerRecords<String, T> records = secondaryConsumer.poll(envConfig.getPollDuration());
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                controlBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
                consumeControlFromBufferAsAReplica();
            }
        }
    }

    private void handleSnapshotBetweenIteration(ConsumerRecord record) {
        int iteration = counter.incrementAndGet();
        if (iteration == envConfig.getIterationBetweenSnapshot()) {
            counter.set(0);
            consumerHandler.processWithSnapshot(ItemToProcess.getItemToProcess(record), status.getCurrentState());
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
        }
    }

    private void processEventsAsAReplica(ConsumerRecord record) {
        ItemToProcess item = ItemToProcess.getItemToProcess(record);
        if (record.key().equals(status.getProcessingKey())) {
            status.setLastProcessedEventOffset(record.offset());
            pollControl();
            if (logger.isDebugEnabled()) {
                logger.debug("processEventsAsAReplica change topic, switch to consume control.");
            }
            consumerHandler.process(item, status.getCurrentState());
            saveOffset(record, primaryConsumer);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
            saveOffset(record, primaryConsumer);
        }
    }

    private  void consumeControlFromBufferAsAReplica() {
        for (ConsumerRecord<String, T> record : controlBuffer) {
            processControlAsAReplica(record);
        }
        controlBuffer = null;
    }

    private  void consumeEventsFromBufferAsAReplica() {
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processEventsAsAReplica(record);
        }
        eventsBuffer = null;
    }

    private  void consumeEventsFromBufferAsALeader() {
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processLeader(record);
        }
        eventsBuffer = null;
    }


    private void processControlAsAReplica(ConsumerRecord record) {

        if (record.offset() == status.getProcessingKeyOffset() + 1 || record.offset() == 0) {
            status.setLastProcessedControlOffset(record.offset());
            status.setProcessingKey(record.key().toString());
            status.setProcessingKeyOffset(record.offset());
            ControlMessage wr = deserialize((byte[]) record.value());
            consumerHandler.processSideEffectsOnReplica(wr.getSideEffects());
            pollEvents();
            if (logger.isDebugEnabled()) {
                logger.debug("change topic, switch to consume events");
            }
        }
        if (status.getProcessingKey() == null) { // empty topic
            status.setProcessingKey(record.key().toString());
            status.setProcessingKeyOffset(record.offset());
        }
        saveOffset(record, secondaryConsumer);
    }

    private  void processLeader(ConsumerRecord record) {

        if (envConfig.isSkipOnDemanSnapshot()) {
            handleSnapshotBetweenIteration(record);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
        }
        status.setProcessingKey(record.key().toString());// the new processed became the new processingKey
        saveOffset(record, primaryConsumer);

        if (logger.isInfoEnabled() || envConfig.isUnderTest()) {
            printer.prettyPrinter("DefaulImprovedKafkaConsumer.processLeader record:{}", record, true);
        }
    }

    private void saveOffset(ConsumerRecord record, Consumer kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        kafkaConsumer.commitSync(map);
    }

    private  void pollControl(){
        status.setPolledTopic(DefaultEventConsumerStatus.PolledTopic.CONTROL);
    }

    private  void pollEvents(){
        status.setPolledTopic(DefaultEventConsumerStatus.PolledTopic.EVENTS);
    }

    private  void startConsume() {
        status.setStarted(true);
    }

    private  void stopConsume() {
        status.setStarted(false);
    }
}
