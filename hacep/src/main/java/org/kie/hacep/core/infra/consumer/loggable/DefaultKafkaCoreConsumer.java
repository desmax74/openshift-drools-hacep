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
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.ItemToProcess;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.util.Printer;
import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;

class DefaultKafkaCoreConsumer<T> implements CoreConsumer {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaCoreConsumer.class);
    private List<ConsumerRecord<String, T>> eventsBuffer;
    private List<ConsumerRecord<String, T>> controlBuffer;
    private ConsumerHandler consumerHandler;
    private AtomicInteger counter;
    private Printer printer;
    private EventConsumerStatus status;
    private EnvConfig envConfig;
    private Consumer<String, T> primaryConsumer, secondaryConsumer;
    private SessionSnapshooter snapShooter;
    private SnapshotInfos snapshotInfos;

    public DefaultKafkaCoreConsumer(ConsumerHandler consumerHandler,
                                    EnvConfig envConfig,
                                    EventConsumerStatus status,
                                    SessionSnapshooter snapShooter) {
        this.status = status;
        this.envConfig = envConfig;
        this.consumerHandler = consumerHandler;
        if (envConfig.isSkipOnDemanSnapshot()) {
            counter = new AtomicInteger(0);
        }
        this.printer = PrinterUtil.getPrinter(this.envConfig);
        this.snapShooter = snapShooter;
    }

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

    public void initConsumer() {
        this.primaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        ;
        if (status.getCurrentState().equals(State.REPLICA)) {
            this.secondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }

    public void stop() {
        primaryConsumer.wakeup();
        if (secondaryConsumer != null) {
            secondaryConsumer.wakeup();
        }
    }

    public void consume() {
        if (status.isStarted()) {
            if (status.getCurrentState().equals(State.LEADER)) {
                defaultProcessAsLeader();
            } else {
                defaultProcessAsAReplica();
            }
        }
    }

    public void assign() {
        if (status.getCurrentState().equals(State.LEADER)) {
            assignAsALeader();
        } else {
            assignReplica();
        }
    }

    public void assignAsALeader() {
        assignConsumer(primaryConsumer,
                       envConfig.getEventsTopicName());
    }

    public void assignReplica() {
        assignConsumer(primaryConsumer,
                       envConfig.getEventsTopicName());
        assignConsumer(secondaryConsumer,
                       envConfig.getControlTopicName());
    }

    public void assignConsumer(Consumer kafkaConsumer,
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
            if (status.getCurrentState().equals(State.LEADER)) {
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        status.getLastProcessedEventOffset()));
            } else if (status.getCurrentState().equals(State.REPLICA)) {
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        status.getLastProcessedControlOffset()));
            }
        }
    }

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
                logger.error(e.getMessage(),
                             e);
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

    public void enableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER)) {
            status.setCurrentState(State.LEADER);
            DroolsExecutor.setAsLeader();
        } else if (state.equals(State.REPLICA)) {
            status.setCurrentState(State.REPLICA);
            secondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
            DroolsExecutor.setAsReplica();
        }
    }

    public void assignAndStartConsume() {
        assign();
        startConsume();
    }

    public void defaultProcessAsLeader() {
        pollEvents();
        ConsumerRecords<String, T> records = primaryConsumer.poll(envConfig.getPollDuration());
        if (!records.isEmpty()) {
            ConsumerRecord<String, T> first = records.iterator().next();
            eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                              first.partition()));
            consumeEventsFromBufferAsALeader();
        } else {
            pollControl();
        }
    }

    public void defaultProcessAsAReplica() {
        if (status.getPolledTopic().equals(DefaultEventConsumerStatus.PolledTopic.EVENTS)) {
            ConsumerRecords<String, T> records = primaryConsumer.poll(envConfig.getPollDuration());
            if (!records.isEmpty()) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                                  first.partition()));
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
                controlBuffer = records.records(new TopicPartition(first.topic(),
                                                                   first.partition()));
                consumeControlFromBufferAsAReplica();
            }
        }
    }

    public void handleSnapshotBetweenIteration(ConsumerRecord record) {
        int iteration = counter.incrementAndGet();
        if (iteration == envConfig.getIterationBetweenSnapshot()) {
            counter.set(0);
            consumerHandler.processWithSnapshot(ItemToProcess.getItemToProcess(record),
                                                status.getCurrentState());
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record),
                                    status.getCurrentState());
        }
    }

    public void processEventsAsAReplica(ConsumerRecord record) {
        ItemToProcess item = ItemToProcess.getItemToProcess(record);
        if (record.key().equals(status.getProcessingKey())) {
            status.setLastProcessedEventOffset(record.offset());
            pollControl();
            if (logger.isDebugEnabled()) {
                logger.debug("processEventsAsAReplica change topic, switch to consume control.");
            }
            consumerHandler.process(item,
                                    status.getCurrentState());
            saveOffset(record,
                       primaryConsumer);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record),
                                    status.getCurrentState());
            saveOffset(record,
                       primaryConsumer);
        }
    }

    public void consumeControlFromBufferAsAReplica() {
        for (ConsumerRecord<String, T> record : controlBuffer) {
            processControlAsAReplica(record);
        }
        controlBuffer = null;
    }

    public void consumeEventsFromBufferAsAReplica() {
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processEventsAsAReplica(record);
        }
        eventsBuffer = null;
    }

    public void consumeEventsFromBufferAsALeader() {
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processLeader(record);
        }
        eventsBuffer = null;
    }

    public void processControlAsAReplica(ConsumerRecord record) {

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
        saveOffset(record,
                   secondaryConsumer);
    }

    public void processLeader(ConsumerRecord record) {

        if (envConfig.isSkipOnDemanSnapshot()) {
            handleSnapshotBetweenIteration(record);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record),
                                    status.getCurrentState());
        }
        status.setProcessingKey(record.key().toString());// the new processed became the new processingKey
        saveOffset(record,
                   primaryConsumer);

        if (logger.isInfoEnabled() || envConfig.isUnderTest()) {
            printer.prettyPrinter("DefaulImprovedKafkaConsumer.processLeader record:{}",
                                  record,
                                  true);
        }
    }

    public void saveOffset(ConsumerRecord record,
                           Consumer kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(),
                                   record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        kafkaConsumer.commitSync(map);
    }

    public void pollControl() {
        status.setPolledTopic(DefaultEventConsumerStatus.PolledTopic.CONTROL);
    }

    public void pollEvents() {
        status.setPolledTopic(DefaultEventConsumerStatus.PolledTopic.EVENTS);
    }

    public void startConsume() {
        status.setStarted(true);
    }

    public void stopConsume() {
        status.setStarted(false);
    }
}
