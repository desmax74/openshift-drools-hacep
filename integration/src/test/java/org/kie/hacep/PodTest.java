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
package org.kie.hacep;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.model.ControlMessage;
import org.kie.hacep.model.SnapshotMessage;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.Config;
import org.kie.remote.EnvConfig;
import org.kie.remote.RemoteCommand;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.RemoteKieSession;
import org.kie.remote.command.InsertCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodTest {

    private final String TEST_KAFKA_LOGGER_TOPIC = "logs";
    private final String TEST_TOPIC = "test";
    private KafkaUtilTest kafkaServerTest;
    private Logger logger = LoggerFactory.getLogger(PodTest.class);
    private EnvConfig config;

    @Before
    public void setUp() throws Exception {
        config = EnvConfig.getDefaultEnvConfig();
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

    @Test
    public void processOneSentMessageAsLeaderTest() {
        Bootstrap.startEngine(config, State.LEADER);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    config.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlConsumerProcessOneSentMessageAsLeaderTest"));
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    config,
                                                    RemoteKieSession.class);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(20000);
            assertEquals(1,
                         eventsRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, byte[]> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(),
                         config.getEventsTopicName());
            RemoteCommand remoteCommand = deserialize(eventsRecord.value());
            assertEquals(eventsRecord.offset(),
                         0);
            assertNotNull(remoteCommand.getId());

            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(20000);
            assertEquals(1,
                         controlRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
            ControlMessage controlMessage = deserialize(controlRecord.value());
            assertEquals(controlRecord.topic(),
                         config.getControlTopicName());
            assertEquals(controlRecord.offset(),
                         0);
            assertTrue(!controlMessage.getSideEffects().isEmpty());

            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(),
                         eventsRecord.key());
        } catch (Exception ex) {
            logger.error(ex.getMessage(),
                         ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }

    @Test
    public void processMessagesAsLeaderAndCreateSnapshotTest() {
        Bootstrap.startEngine(config, State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer snapshotConsumer = kafkaServerTest.getConsumer("",
                                                                     config.getSnapshotTopicName(),
                                                                     Config.getSnapshotConsumerConfig());
        kafkaServerTest.insertBatchStockTicketEvent(10,
                                                    config,
                                                    RemoteKieSession.class);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(20000);
            assertEquals(10,
                         eventsRecords.count());

            //SNAPSHOT TOPIC
            ConsumerRecords snapshotRecords = snapshotConsumer.poll(20000);
            assertEquals(1,
                         snapshotRecords.count());
            ConsumerRecord record = (ConsumerRecord) snapshotRecords.iterator().next();
            SnapshotMessage snapshot = deserialize((byte[]) record.value());
            assertNotNull(snapshot);
            assertTrue(snapshot.getLastInsertedEventOffset() > 0);
            assertFalse(snapshot.getFhMapKeys().isEmpty());
            assertNotNull(snapshot.getLastInsertedEventkey());
            assertTrue(snapshot.getFhMapKeys().size() == 9);
            assertNotNull(snapshot.getLastInsertedEventkey());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            snapshotConsumer.close();
        }
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
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    config,
                                                    RemoteKieSession.class);
        try {

            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(20000);
            assertEquals(1,
                         eventsRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, byte[]> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(),
                         config.getEventsTopicName());
            RemoteCommand remoteCommand = deserialize(eventsRecord.value());
            assertEquals(eventsRecord.offset(),
                         0);
            assertNotNull(remoteCommand.getId());
            InsertCommand insertCommand = (InsertCommand) remoteCommand;
            assertEquals(insertCommand.getEntryPoint(),
                         "DEFAULT");
            assertNotNull(insertCommand.getId());
            assertNotNull(insertCommand.getFactHandle());
            RemoteFactHandle remoteFactHandle = insertCommand.getFactHandle();
            StockTickEvent eventsTicket = (StockTickEvent) remoteFactHandle.getObject();
            assertEquals(eventsTicket.getCompany(),
                         "RHT");

            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(20000);
            assertEquals(1,
                         controlRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
            assertEquals(controlRecord.topic(),
                         config.getControlTopicName());
            ControlMessage controlMessage = deserialize(controlRecord.value());
            assertEquals(controlRecord.offset(),
                         0);
            assertTrue(!controlMessage.getSideEffects().isEmpty());

            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(),
                         eventsRecord.key());

            //no more msg to consume as a leader
            eventsRecords = eventsConsumer.poll(20000);
            assertEquals(0,
                         eventsRecords.count());
            controlRecords = controlConsumer.poll(20000);
            assertEquals(0,
                         controlRecords.count());

            // SWITCH AS a REPLICA
            Bootstrap.getConsumerController().getCallback().updateStatus(State.REPLICA);
            //@TODO with kafka logger

        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }
}
