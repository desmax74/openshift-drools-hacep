package org.kie.hacep;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.model.ControlMessage;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.Config;
import org.kie.remote.EnvConfig;
import org.kie.remote.RemoteCommand;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.RemoteKieSession;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.util.PrinterLogImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodTestWithKafkaLogger {

    private final String TEST_KAFKA_LOGGER_TOPIC = "logs";
    private final String TEST_TOPIC = "test";
    private KafkaUtilTest kafkaServerTest;
    private Logger logger = LoggerFactory.getLogger(PodTestWithKafkaLogger.class);
    private Logger kafkaLogger = LoggerFactory.getLogger("org.hacep");
    private EnvConfig config;

    @Before
    public void setUp() throws Exception {
        config = getEnvConfig();
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.createTopic(TEST_TOPIC);
        kafkaServerTest.createTopic(config.getEventsTopicName());
        kafkaServerTest.createTopic(config.getControlTopicName());
        kafkaServerTest.createTopic(config.getSnapshotTopicName());
        kafkaServerTest.createTopic(config.getKieSessionInfosTopicName());
    }

    @After
    public void tearDown() {
        try {
            Bootstrap.stopEngine();
        } catch (ConcurrentModificationException ex) {
        }
        kafkaServerTest.deleteTopic(TEST_TOPIC);
        kafkaServerTest.deleteTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.deleteTopic(config.getEventsTopicName());
        kafkaServerTest.deleteTopic(config.getControlTopicName());
        kafkaServerTest.deleteTopic(config.getSnapshotTopicName());
        kafkaServerTest.deleteTopic(config.getKieSessionInfosTopicName());
        kafkaServerTest.shutdownServer();
    }

    private EnvConfig getEnvConfig(){
        return EnvConfig.anEnvConfig().
                withNamespace(Optional.ofNullable(System.getenv( Config.NAMESPACE)).orElse(Config.DEFAULT_NAMESPACE)).
                withControlTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_CONTROL_TOPIC)).orElse(Config.DEFAULT_CONTROL_TOPIC)).
                withEventsTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_EVENTS_TOPIC)).orElse(Config.DEFAULT_EVENTS_TOPIC)).
                withSnapshotTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_SNAPSHOT_TOPIC)).orElse(Config.DEFAULT_SNAPSHOT_TOPIC)).
                withKieSessionInfosTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_KIE_SESSION_INFOS_TOPIC)).orElse(Config.DEFAULT_KIE_SESSION_INFOS_TOPIC)).
                withPrinterType(Optional.ofNullable(PrinterKafkaImpl.class.getName()).orElse(PrinterLogImpl.class.getName())).build();
    }


    @Test
    public void processOneSentMessageAsLeaderAndThenReplicaTest() {
        Bootstrap.startEngine(config, State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    config.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlConsumerProcessOneSentMessageAsLeaderTest"));

        KafkaConsumer<byte[], String> kafkaLogConsumer = kafkaServerTest.getStringConsumer(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.insertBatchStockTicketEvent(1, config, RemoteKieSession.class);
        try {

            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(1, eventsRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, byte[]> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(), config.getEventsTopicName());
            RemoteCommand remoteCommand = deserialize(eventsRecord.value());
            assertEquals(eventsRecord.offset(), 0);
            assertNotNull(remoteCommand.getId());
            InsertCommand insertCommand = (InsertCommand) remoteCommand;
            assertEquals(insertCommand.getEntryPoint(), "DEFAULT");
            assertNotNull(insertCommand.getId());
            assertNotNull(insertCommand.getFactHandle());
            RemoteFactHandle remoteFactHandle = insertCommand.getFactHandle();
            StockTickEvent eventsTicket = (StockTickEvent) remoteFactHandle.getObject();
            assertEquals(eventsTicket.getCompany(),
                         "RHT");

            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(2000);
            assertEquals(1, controlRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
            assertEquals(controlRecord.topic(), config.getControlTopicName());
            ControlMessage controlMessage = deserialize(controlRecord.value());
            assertEquals(controlRecord.offset(), 0);
            assertTrue(!controlMessage.getSideEffects().isEmpty());

            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(), eventsRecord.key());

            //no more msg to consume as a leader
            eventsRecords = eventsConsumer.poll(2000);
            assertEquals(0, eventsRecords.count());
            controlRecords = controlConsumer.poll(2000);
            assertEquals(0, controlRecords.count());

            // SWITCH AS a REPLICA
            Bootstrap.getConsumerController().getCallback().updateStatus(State.REPLICA);
            kafkaServerTest.insertBatchStockTicketEvent(1, config, RemoteKieSession.class);

            ConsumerRecords<byte[], String> recordsLog = kafkaLogConsumer.poll(5000);
            Iterator<ConsumerRecord<byte[], String>> recordIterator = recordsLog.iterator();
            List<String> kafkaLoggerMsgs = new ArrayList();
            while (recordIterator.hasNext()){
                ConsumerRecord<byte[], String> record = recordIterator.next();
                kafkaLoggerMsgs.add(record.value());
            }
            for(String item: kafkaLoggerMsgs){
                if(item.startsWith("sideEffectOn")){
                    if(item.endsWith(":null")){
                        fail("SideEffects null");
                    }
                }
            }

        } catch (Exception ex) {
            logger.error(ex.getMessage(),
                         ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
            kafkaLogConsumer.close();
        }
    }
}
