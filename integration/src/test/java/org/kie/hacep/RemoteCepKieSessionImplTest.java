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

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.remote.RemoteCepKieSession;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.PrinterLogImpl;
import org.kie.hacep.model.StockTickEvent;
import org.kie.hacep.producer.RemoteCepKieSessionImpl;

import static org.junit.Assert.*;

public class RemoteCepKieSessionImplTest {

    private KafkaUtilTest kafkaServerTest;
    private EnvConfig config;

    @Before
    public void setUp() throws Exception {
        config = EnvConfig.getDefaultEnvConfig();
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
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
        kafkaServerTest.deleteTopic(config.getEventsTopicName());
        kafkaServerTest.deleteTopic(config.getControlTopicName());
        kafkaServerTest.deleteTopic(config.getSnapshotTopicName());
        kafkaServerTest.deleteTopic(config.getKieSessionInfosTopicName());
        kafkaServerTest.shutdownServer();
    }

    @Test
    public void getFactCountTest() throws Exception {
        Bootstrap.startEngine(new PrinterLogImpl(),
                              config,
                              State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(7,
                                                    config,
                                                    RemoteCepKieSession.class);
        try (RemoteCepKieSessionImpl client = new RemoteCepKieSessionImpl(Config.getProducerConfig("FactCountConsumerTest"),
                                                                          config)) {
            client.listen();
            CompletableFuture<Long> factCountFuture = client.getFactCount();
            Object cfutureValue = factCountFuture.get(15, TimeUnit.SECONDS);
            Long factCount = (Long) cfutureValue;
            assertTrue(factCount == 7);
        }
    }

    @Test
    public void getListKieSessionObjectsTest() throws Exception {
        Bootstrap.startEngine(new PrinterLogImpl(),
                              config,
                              State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    config,
                                                    RemoteCepKieSession.class);
        try (RemoteCepKieSessionImpl client = new RemoteCepKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsConsumerTest"),
                                                                          config)) {
            client.listen();
            CompletableFuture<Collection<? extends Object>> listKieObjectsFuture = client.getObjects();
            Object cfutureValue = listKieObjectsFuture.get(15,
                                                         TimeUnit.SECONDS);
            Collection<? extends Object> listKieObjects = (Collection<? extends Object>) cfutureValue;
            assertTrue(listKieObjects.size() == 1);
            Object obj = listKieObjects.iterator().next();
            StockTickEvent event = (StockTickEvent) obj;
            assertTrue(event.getCompany().equals("RHT"));
        }
    }

    @Test
    public void getListKieSessionObjectsWithClassTypeTest() throws Exception {
        Bootstrap.startEngine(new PrinterLogImpl(),
                              config,
                              State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    config,
                                                    RemoteCepKieSession.class);
        try (RemoteCepKieSessionImpl client = new RemoteCepKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsWithClassTypeTest"),
                                                                          config)) {
            client.listen();
            CompletableFuture<Collection<? extends Object>> listKieObjectsFuture = client.getObjects(StockTickEvent.class);
            Object cfutureValueValue = listKieObjectsFuture.get(15, TimeUnit.SECONDS);
            Collection<? extends Object> listKieObjects = (Collection<? extends Object>) cfutureValueValue;
            assertTrue(listKieObjects.size() == 1);
            Object obj = listKieObjects.iterator().next();
            StockTickEvent event = (StockTickEvent) obj;
            assertTrue(event.getCompany().equals("RHT"));
        }
    }

    @Test
    public void getListKieSessionObjectsWithNamedQueryTest() throws Exception {
        Bootstrap.startEngine(new PrinterLogImpl(), config, State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    config,
                                                    RemoteCepKieSession.class);
        try (RemoteCepKieSessionImpl client = new RemoteCepKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsWithNamedQueryTest"),
                                                                          config)) {
            client.listen();

            CompletableFuture<Collection<? extends Object>> listKieObjectsFuture = client.getObjects("stockTickEventQuery" , "stock", new Object[]{"IBM"});
            Object listKieObjectsValue = listKieObjectsFuture.get(15, TimeUnit.SECONDS);
            Collection<? extends Object> listKieObjects = (Collection<? extends Object>) listKieObjectsValue;
            assertTrue(listKieObjects.size() == 0);


            listKieObjectsFuture = client.getObjects("stockTickEventQuery" , "stock", new Object[]{"RHT"});
            listKieObjectsValue = listKieObjectsFuture.get(15, TimeUnit.SECONDS);
            listKieObjects = (Collection<? extends Object>) listKieObjectsValue;
            assertTrue(listKieObjects.size() == 1);
            Object obj = listKieObjects.iterator().next();
            StockTickEvent event = (StockTickEvent) obj;
            assertTrue(event.getCompany().equals("RHT"));
        }
    }

}
