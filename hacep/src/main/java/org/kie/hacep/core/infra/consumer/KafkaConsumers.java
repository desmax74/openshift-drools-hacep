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
import org.kie.hacep.core.infra.OffsetManager;
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

public class KafkaConsumers<T> {

    private Logger logger = LoggerFactory.getLogger(KafkaConsumers.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsEvents = new HashMap<>();
    private Consumer<String, T> kafkaConsumer, kafkaSecondaryConsumer;
    private EventConsumerLifecycleProxy proxy;
    private EnvConfig config;
    private SnapshotInfos snapshotInfos;
    private SessionSnapshooter snapShooter;
    private volatile long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;
    private volatile String processingKey = "";
    private EventConsumerStatus status;
    protected List<ConsumerRecord<String,T>> eventsBuffer;
    protected List<ConsumerRecord<String,T>> controlBuffer;
    private Logger loggerForTest;
    private DroolsConsumerHandler consumerHandler;
    private int iterationBetweenSnapshot;
    private AtomicInteger counter ;
    private Printer printer;



    public KafkaConsumers(EventConsumerStatus status, EnvConfig config, EventConsumerLifecycleProxy proxy, ConsumerHandler consumerHandler, SessionSnapshooter snapshooter){
        this.proxy = proxy;
        this.config = config;
        this.snapShooter = snapshooter;
        this.status = status;
        this.consumerHandler = (DroolsConsumerHandler) consumerHandler;
        if (this.config.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(this.config);
        }
        if(this.config.isSkipOnDemanSnapshot()){
            counter = new AtomicInteger(0);
        }
        this.printer = PrinterUtil.getPrinter(this.config);
        iterationBetweenSnapshot = this.config.getIterationBetweenSnapshot();
    }

    public void initConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        if (proxy.getStatus().getCurrentState().equals(State.REPLICA)) {
            this.kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }


    public void stop() {
        kafkaConsumer.wakeup();
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.wakeup();
        }
    }

    public void poll(int durationMillis) {

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
            while (!proxy.getStatus().isExit()) {
                consume(durationMillis);
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
            } catch (WakeupException e) {
                //nothing to do
            } finally {
                logger.info("Closing kafkaConsumer on the loop");
                kafkaConsumer.close();
                kafkaSecondaryConsumer.close();
            }
        }
    }


    private  void consume(int millisTimeout) {
        if (status.isStarted()) {
            if (status.getCurrentState().equals(State.LEADER)) {
                defaultProcessAsLeader(millisTimeout);
            } else {
                defaultProcessAsAReplica(millisTimeout);
            }
        }
    }

    public void restartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        assign();
        if (proxy.getStatus().getCurrentState().equals(State.REPLICA)) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        } else {
            kafkaSecondaryConsumer = null;
        }
    }

    private  void assign() {
        if (proxy.getStatus().getCurrentState().equals(State.LEADER)) {
            assignAsALeader();
        } else {
            assignReplica();
        }
    }

    private  void assignAsALeader() {
        assignConsumer(kafkaConsumer, config.getEventsTopicName());
    }

    private void assignReplica() {
        assignConsumer(kafkaConsumer, config.getEventsTopicName());
        assignConsumer(kafkaSecondaryConsumer, config.getControlTopicName());
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
            if(proxy.getStatus().getCurrentState().equals(State.LEADER)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedEventOffset));
            }else if(proxy.getStatus().getCurrentState().equals(State.REPLICA)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedControlOffset));
            }
        }
    }

    public  void enableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER)) {
            status.setCurrentState(State.LEADER);
            DroolsExecutor.setAsLeader();

        } else if (state.equals(State.REPLICA) ) {
            status.setCurrentState(State.REPLICA);
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
            DroolsExecutor.setAsReplica();
        }
        setLastProcessedKey();
        assignAndStartConsume();
    }

    public  void setLastProcessedKey() {
        ControlMessage lastControlMessage = ConsumerUtils.getLastEvent(config.getControlTopicName(), config.getPollTimeout());
        settingsOnAEmptyControlTopic(lastControlMessage);
        processingKey = lastControlMessage.getId();
        processingKeyOffset = lastControlMessage.getOffset();
    }

    public  void settingsOnAEmptyControlTopic(ControlMessage lastWrapper) {
        if (lastWrapper.getId() == null) {// completely empty or restart of ephemeral already used
            if (proxy.getStatus().getCurrentState().equals(State.REPLICA)) {
                pollControl();
            }
        }
    }

    public void internalRestartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?

        restartConsumer();
    }

    public  void restart(State state) {
        stopConsume();
        internalRestartConsumer();
        enableConsumeAndStartLoop(state);
    }

    private  void assignAndStartConsume() {
        assign();
        startConsume();
    }


    private  void defaultProcessAsLeader(int millisTimeout) {
        pollEvents();
        if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
            consumeEventsFromBufferAsALeader();
        }
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(millisTimeout, ChronoUnit.MILLIS));
        if (!records.isEmpty()) {
            ConsumerRecord<String, T> first = records.iterator().next();
            eventsBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
            consumeEventsFromBufferAsALeader();
        } else {
            pollControl();
        }
    }

    private void defaultProcessAsAReplica(int millisTimeout) {
        if (status.getPolledTopic().equals(EventConsumerStatus.PolledTopic.EVENTS)) {
            if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
                consumeEventsFromBufferAsAReplica();
            }
            ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(millisTimeout,
                                                                                ChronoUnit.MILLIS));
            if (!records.isEmpty()) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                                  first.partition()));
                consumeEventsFromBufferAsAReplica();
            } else {
                pollControl();
            }
        }

        if (status.getPolledTopic().equals(EventConsumerStatus.PolledTopic.CONTROL)) {

            if (controlBuffer != null && controlBuffer.size() > 0) {
                consumeControlFromBufferAsAReplica();
            }

            ConsumerRecords<String, T> records = kafkaSecondaryConsumer.poll(Duration.of(millisTimeout,
                                                                                         ChronoUnit.MILLIS));
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                controlBuffer = records.records(new TopicPartition(first.topic(),
                                                                   first.partition()));
                consumeControlFromBufferAsAReplica();
            }
        }
    }



    private void handleSnapshotBetweenIteration(ConsumerRecord record) {
        int iteration = counter.incrementAndGet();
        if (iteration == iterationBetweenSnapshot) {
            counter.set(0);
            consumerHandler.processWithSnapshot(ItemToProcess.getItemToProcess(record), status.getCurrentState());
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
        }
    }

    private void processEventsAsAReplica(ConsumerRecord record) {

        ItemToProcess item = ItemToProcess.getItemToProcess(record);
        if (record.key().equals(processingKey)) {
            lastProcessedEventOffset = record.offset();

            pollControl();

            if (logger.isDebugEnabled()) {
                logger.debug("processEventsAsAReplica change topic, switch to consume control, still {} events in the eventsBuffer to consume and processing item:{}.", eventsBuffer.size(), item );
            }
            consumerHandler.process(item, status.getCurrentState());
            saveOffset(record, kafkaConsumer);

        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("processEventsAsAReplica still {} events in the eventsBuffer to consume and processing item:{}.", eventsBuffer.size(), item );
            }
            consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
            saveOffset(record, kafkaConsumer);
        }
    }

    private  void consumeControlFromBufferAsAReplica() {
        for (ConsumerRecord<String, T> record : controlBuffer) {
            processControlAsAReplica(record);
        }
        controlBuffer = null;
    }

    private  void consumeEventsFromBufferAsAReplica() {
        if (config.isUnderTest()) {
            loggerForTest.warn("consumeEventsFromBufferAsAReplica eventsBufferSize:{}", eventsBuffer.size());
        }
        int index = 0;
        int end = eventsBuffer.size();
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processEventsAsAReplica(record);
            index++;
            if (status.getPolledTopic().equals(EventConsumerStatus.PolledTopic.CONTROL)) {
                if (end > index) {
                    eventsBuffer = eventsBuffer.subList(index, end);
                }
                break;
            }
        }
        if (end == index) {
            eventsBuffer = null;
        }
    }


    private void processControlAsAReplica(ConsumerRecord record) {

        if (record.offset() == processingKeyOffset + 1 || record.offset() == 0) {
            lastProcessedControlOffset = record.offset();
            processingKey = record.key().toString();
            processingKeyOffset = record.offset();
            ControlMessage wr = deserialize((byte[]) record.value());
            consumerHandler.processSideEffectsOnReplica(wr.getSideEffects());

            pollEvents();
            if (logger.isDebugEnabled()) {
                logger.debug("change topic, switch to consume events");
            }
        }
        if (processingKey == null) { // empty topic
            processingKey = record.key().toString();
            processingKeyOffset = record.offset();
        }
        saveOffset(record, kafkaSecondaryConsumer);
    }


    private  void consumeEventsFromBufferAsALeader() {
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processLeader(record);

        }
        eventsBuffer = null;
    }

    private  void processLeader(ConsumerRecord record) {

        if (config.isSkipOnDemanSnapshot()) {
            handleSnapshotBetweenIteration(record);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
        }
        processingKey = record.key().toString();// the new processed became the new processingKey
        saveOffset(record, kafkaConsumer);

        if (logger.isInfoEnabled() || config.isUnderTest()) {
            printer.prettyPrinter("DefaulImprovedKafkaConsumer.processLeader record:{}", record, true);
        }
    }

    private void saveOffset(ConsumerRecord record, Consumer kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(),
                                   record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        kafkaConsumer.commitSync(map);
    }

    private  void pollControl(){
        if(!status.getPolledTopic().equals(EventConsumerStatus.PolledTopic.CONTROL)) {
            status.setPolledTopic(EventConsumerStatus.PolledTopic.CONTROL);
        }
    }

    private  void pollEvents(){
        if(!status.getPolledTopic().equals(EventConsumerStatus.PolledTopic.EVENTS)) {
            status.setPolledTopic(EventConsumerStatus.PolledTopic.EVENTS);
        }
    }

    private  void startConsume() {
        status.setStarted(true);
    }

    private  void stopConsume() {
        status.setStarted(false);
    }
}
