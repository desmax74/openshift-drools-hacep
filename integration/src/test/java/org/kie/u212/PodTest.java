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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.u212.core.Bootstrap;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.utils.PrinterLogImpl;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
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
        kafkaServerTest.shutdownServer();
    }

    @Test
    public void processOneSentMessageAsLeader() {
        Bootstrap.startEngine(new PrinterLogImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("", config.getEventsTopicName(), Config.getConsumerConfig());
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",config.getControlTopicName(), Config.getConsumerConfig());
        kafkaServerTest.insertBatchStockTicketEvent(1, config);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(1, eventsRecords.count());
            Iterator<ConsumerRecord<String, EventWrapper>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, EventWrapper> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(), config.getEventsTopicName());
            assertEquals(eventsRecord.value().getOffset(),0);
            assertEquals(eventsRecord.value().getEventType(), EventType.APP);
            assertEquals(eventsRecord.value().getSideEffects().size(), 0);
            Map map = (Map) eventsRecord.value().getDomainEvent();
            StockTickEvent eventsTicket = ConverterUtil.fromMap(map);
            assertEquals(eventsTicket.getCompany(), "RHT");

            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(5000);
            assertEquals(1, controlRecords.count());
            Iterator<ConsumerRecord<String, EventWrapper>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, EventWrapper> controlRecord = controlRecordIterator.next();
            assertEquals(controlRecord.topic(), config.getControlTopicName());
            assertEquals(controlRecord.value().getOffset(),0);
            assertEquals(controlRecord.value().getEventType(), EventType.APP);
            assertTrue(!controlRecord.value().getSideEffects().isEmpty());

            Map mapControl = (Map) controlRecord.value().getDomainEvent();
            StockTickEvent controlTicket = ConverterUtil.fromMap(mapControl);

            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(), eventsRecord.key());
            assertEquals(controlTicket.getCompany(), eventsTicket.getCompany());
            assertTrue(controlTicket.getPrice() == eventsTicket.getPrice());

        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }

    @Test
    public void processMessagesAsLeaderAndCreateSnapshot() {
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

        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            snapshotConsumer.close();
        }
    }

    @Test
    public void processOneSentMessageAsLeaderAndThenReplica() {
        Bootstrap.startEngine(new PrinterKafkaImpl(), config);
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("", config.getEventsTopicName(), Config.getConsumerConfig());
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("", config.getControlTopicName(), Config.getConsumerConfig());
        kafkaServerTest.insertBatchStockTicketEvent(1, config);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(1, eventsRecords.count());
            Iterator<ConsumerRecord<String, EventWrapper>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, EventWrapper> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(),  config.getEventsTopicName());
            assertEquals(eventsRecord.value().getOffset(),0);
            assertEquals(eventsRecord.value().getEventType(), EventType.APP);
            assertEquals(eventsRecord.value().getSideEffects().size(), 0);
            Map map = (Map) eventsRecord.value().getDomainEvent();
            StockTickEvent eventsTicket = ConverterUtil.fromMap(map);
            assertEquals(eventsTicket.getCompany(), "RHT");

            //CONTROL TOPIC
            ConsumerRecords controlRecords = controlConsumer.poll(5000);
            assertEquals(1, controlRecords.count());
            Iterator<ConsumerRecord<String, EventWrapper>> controlRecordIterator = controlRecords.iterator();
            ConsumerRecord<String, EventWrapper> controlRecord = controlRecordIterator.next();
            assertEquals(controlRecord.topic(), config.getControlTopicName());
            assertEquals(controlRecord.value().getOffset(),0);
            assertEquals(controlRecord.value().getEventType(), EventType.APP);
            assertTrue(!controlRecord.value().getSideEffects().isEmpty());

            //EventWrapper<StockTickEvent> cew = (EventWrapper<StockTickEvent>) controlRecord.value().getDomainEvent();
            //StockTickEvent controlTicket = cew.getDomainEvent();
            Map mapControl = (Map) controlRecord.value().getDomainEvent();
            StockTickEvent controlTicket = ConverterUtil.fromMap(mapControl);

            //Same msg content on Events topic and control topics
            assertEquals(controlRecord.key(), eventsRecord.key());
            assertEquals(controlTicket.getCompany(), eventsTicket.getCompany());
            assertTrue(controlTicket.getPrice() == eventsTicket.getPrice());

            //no more msg to consume as a leader
            eventsRecords = eventsConsumer.poll(5000);
            assertEquals(0, eventsRecords.count());
            controlRecords = controlConsumer.poll(5000);
            assertEquals(0, controlRecords.count());

            // SWITCH AS a REPLICA
            Bootstrap.getRestarter().getCallback().updateStatus(State.NOT_LEADER);
            //@TODO with kafka logger

        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }


}
