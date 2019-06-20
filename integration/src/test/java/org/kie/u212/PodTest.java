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
package org.kie.u212;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.remote.RemoteCommand;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.InsertCommand;
import org.kie.u212.core.Bootstrap;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.utils.PrinterLogImpl;
import org.kie.u212.model.ControlMessage;
import org.kie.u212.model.SnapshotMessage;
import org.kie.u212.model.StockTickEvent;
import org.kie.u212.producer.RemoteKieSessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class PodTest {

    private KafkaUtilTest kafkaServerTest;
    private Logger logger = LoggerFactory.getLogger(PodTest.class);
    private final  String TEST_KAFKA_LOGGER_TOPIC = "logs";
    private final  String TEST_TOPIC = "test";
    private EnvConfig config;


    @Before
    public void setUp() throws Exception{
        config= EnvConfig.getDefaultEnvConfig();
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
    public void tearDown(){
        try {
            Bootstrap.stopEngine();
        }catch (ConcurrentModificationException ex){ }
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
        Bootstrap.startEngine(new PrinterLogImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("", config.getEventsTopicName(), Config.getConsumerConfig());
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",config.getControlTopicName(), Config.getConsumerConfig());
        kafkaServerTest.insertBatchStockTicketEvent(1, config);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(1, eventsRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, byte[]> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(), config.getEventsTopicName());
            RemoteCommand remoteCommand = ConverterUtil.deSerializeObjInto(eventsRecord.value(), RemoteCommand.class);
            assertEquals(eventsRecord.offset(),0);
            assertNotNull(remoteCommand.getId());

            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(5000);
            assertEquals(1, controlRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
            ControlMessage controlMessage= ConverterUtil.deSerializeObjInto(controlRecord.value(), ControlMessage.class);
            assertEquals(controlRecord.topic(), config.getControlTopicName());
            assertEquals(controlRecord.offset(),0);
            assertTrue(!controlMessage.getSideEffects().isEmpty());

            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(), eventsRecord.key());

        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }

    @Test
    public void processMessagesAsLeaderAndCreateSnapshotTest() {
        Bootstrap.startEngine(new PrinterLogImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("", config.getEventsTopicName(), Config.getConsumerConfig());
        KafkaConsumer snapshotConsumer = kafkaServerTest.getConsumer("",config.getSnapshotTopicName(), Config.getSnapshotConsumerConfig());
        kafkaServerTest.insertBatchStockTicketEvent(10, config);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(10, eventsRecords.count());

            //SNAPSHOT TOPIC
            ConsumerRecords snapshotRecords = snapshotConsumer.poll(5000);
            assertEquals(1, snapshotRecords.count());
            ConsumerRecord record = (ConsumerRecord) snapshotRecords.iterator().next();
            SnapshotMessage snapshot = ConverterUtil.deSerializeObjInto((byte[])record.value(),
                                              SnapshotMessage.class);
            assertNotNull(snapshot);
            assertTrue(snapshot.getLastInsertedEventOffset() > 0);
            assertFalse(snapshot.getFhMapKeys().isEmpty());
            assertNotNull(snapshot.getLastInsertedEventkey());
            assertTrue(snapshot.getFhMapKeys().size()== 9);
            assertNotNull(snapshot.getLastInsertedEventkey());

        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            snapshotConsumer.close();
        }
    }


    @Test
    public void processOneSentMessageAsLeaderAndThenReplicaTest() {
        Bootstrap.startEngine(new PrinterKafkaImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("", config.getEventsTopicName(), Config.getConsumerConfig());
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("", config.getControlTopicName(), Config.getConsumerConfig());
        kafkaServerTest.insertBatchStockTicketEvent(1, config);
        try {

            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(1, eventsRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, byte[]> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(), config.getEventsTopicName());
            RemoteCommand remoteCommand = ConverterUtil.deSerializeObjInto(eventsRecord.value(), RemoteCommand.class);
            assertEquals(eventsRecord.offset(),0);
            assertNotNull(remoteCommand.getId());
            InsertCommand insertCommand = (InsertCommand)remoteCommand;
            assertEquals(insertCommand.getEntryPoint(), "DEFAULT");
            assertNotNull(insertCommand.getId());
            assertNotNull(insertCommand.getFactHandle());
            RemoteFactHandle remoteFactHandle = insertCommand.getFactHandle();
            StockTickEvent eventsTicket = (StockTickEvent) remoteFactHandle.getObject();
            assertEquals(eventsTicket.getCompany(), "RHT");


            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(5000);
            assertEquals(1, controlRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
            assertEquals(controlRecord.topic(), config.getControlTopicName());
            ControlMessage controlMessage = ConverterUtil.deSerializeObjInto(controlRecord.value(), ControlMessage.class);
            assertEquals(controlRecord.offset(),0);
            assertTrue(!controlMessage.getSideEffects().isEmpty());


            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(), eventsRecord.key());


            //no more msg to consume as a leader
            eventsRecords = eventsConsumer.poll(5000);
            assertEquals(0, eventsRecords.count());
            controlRecords = controlConsumer.poll(5000);
            assertEquals(0, controlRecords.count());

            // SWITCH AS a REPLICA
            Bootstrap.getRestarter().getCallback().updateStatus(State.REPLICA);
            //@TODO with kafka logger

        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }


    @Test
    public void getFactCountTest() throws Exception{
        Bootstrap.startEngine(new PrinterLogImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(7, config);
        try (RemoteKieSessionImpl client = new RemoteKieSessionImpl(Config.getProducerConfig(), config)) {
            CompletableFuture<Long> factCount = client.getFactCount();
            assertTrue(factCount.get(1, TimeUnit.SECONDS) == 7);
        }
    }

    @Test
    public void getFactCountAsReplicaTest() throws Exception{
        Bootstrap.startEngine(new PrinterLogImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.REPLICA);
        kafkaServerTest.insertBatchStockTicketEvent(7, config);
        try (RemoteKieSessionImpl client = new RemoteKieSessionImpl(Config.getProducerConfig(), config)) {
            CompletableFuture<Long> factCount = client.getFactCount();
            assertTrue(factCount.get(2, TimeUnit.SECONDS) == 0);
        }
    }

    @Test
    public void getObjectAsLeaderTest() throws Exception{
        Bootstrap.startEngine(new PrinterLogImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(7, config);
        try (RemoteKieSessionImpl client = new RemoteKieSessionImpl(Config.getProducerConfig(), config)) {
            CompletableFuture<Collection<? extends Object>> objects = client.getObjects();
            assertTrue(objects.get(1, TimeUnit.SECONDS).size() == 7);
        }
    }




}
