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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.InfraFactory;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.CommonConfig;
import org.kie.remote.RemoteStreamingKieSession;
import org.kie.remote.TopicsConfig;
import org.kie.remote.impl.RemoteStreamingKieSessionImpl;
import org.kie.remote.impl.consumer.Listener;
import org.kie.remote.impl.consumer.ListenerThread;
import org.kie.remote.impl.producer.Producer;

import static org.junit.Assert.*;
import static org.kie.remote.CommonConfig.getTestProperties;

public class RemoteStreamingKieSessionImplTest extends KafkaFullTopicsTests{

    @Test
    @Ignore
    public void createTest(){
        TopicsConfig topicsConfig = TopicsConfig.getDefaultTopicsConfig();
        ListenerThread listenerThread = InfraFactory.getListenerThread(TopicsConfig.getDefaultTopicsConfig(),false, getTestProperties());
        Listener listener = new Listener(getTestProperties(), listenerThread);
        RemoteStreamingKieSession session = InfraFactory.createRemoteStreamingKieSession(getTestProperties(), topicsConfig, listenerThread, InfraFactory.getProducer(false));//RemoteStreamingKieSession.create(getTestProperties(), topicsConfig);
        assertNotNull(session);
    }

    @Test
    public void getFactCountTest() throws Exception {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(7, topicsConfig, RemoteStreamingKieSession.class);
        ListenerThread listenerThread = InfraFactory.getListenerThread(TopicsConfig.getDefaultTopicsConfig(), false, getTestProperties());
        Listener listener = new Listener(getTestProperties(), listenerThread);
        Producer prod = InfraFactory.getProducer(false);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(Config.getProducerConfig("FactCountConsumerTest"),
                                                                     topicsConfig, listenerThread, prod);
        try {
            CompletableFuture<Long> factCountFuture = client.getFactCount();
            Long factCount = factCountFuture.get(5, TimeUnit.SECONDS);
            assertTrue(factCount == 7);
        }finally {
            client.close();
        }
    }

    @Test
    public void getListKieSessionObjectsTest() throws Exception {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1, topicsConfig, RemoteStreamingKieSession.class);
        ListenerThread listenerThread = InfraFactory.getListenerThread(TopicsConfig.getDefaultTopicsConfig(),false, getTestProperties());
        Listener listener = new Listener(getTestProperties(), listenerThread);
        Producer prod = InfraFactory.getProducer(false);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(CommonConfig.getProducerConfig("ListKieSessionObjectsConsumerTest"),
                                                                     topicsConfig, listenerThread, prod);
        try {
            CompletableFuture<Collection> listKieObjectsFuture = client.getObjects();
            Collection listKieObjects = listKieObjectsFuture.get(5, TimeUnit.SECONDS);
            assertTrue(listKieObjects.size() == 1);
            StockTickEvent event = (StockTickEvent) listKieObjects.iterator().next();
            assertTrue(event.getCompany().equals("RHT"));
        }finally {
            client.close();
        }

    }

    @Test
    public void getListKieSessionObjectsWithClassTypeTest() throws Exception {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1, topicsConfig, RemoteStreamingKieSession.class);
        ListenerThread listenerThread = InfraFactory.getListenerThread(TopicsConfig.getDefaultTopicsConfig(),false, getTestProperties());
        Listener listener = new Listener(getTestProperties(), listenerThread);
        Producer prod = InfraFactory.getProducer(false);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsWithClassTypeTest"),
                                                                     topicsConfig, listenerThread, prod);
        try {
            CompletableFuture<Collection<StockTickEvent>> listKieObjectsFuture = client.getObjects(StockTickEvent.class);
            Collection<? extends Object> listKieObjects = listKieObjectsFuture.get(5, TimeUnit.SECONDS);
            assertTrue(listKieObjects.size() == 1);
            StockTickEvent event = (StockTickEvent) listKieObjects.iterator().next();
            assertTrue(event.getCompany().equals("RHT"));
        }finally {
            client.close();
        }
    }

    @Test
    public void getListKieSessionObjectsWithNamedQueryTest() throws Exception {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1, topicsConfig, RemoteStreamingKieSession.class);
        ListenerThread listenerThread = InfraFactory.getListenerThread(TopicsConfig.getDefaultTopicsConfig(),false, getTestProperties());
        Listener listener = new Listener(getTestProperties(), listenerThread);
        Producer prod = InfraFactory.getProducer(false);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsWithNamedQueryTest"),
                                                                     topicsConfig, listenerThread, prod);
        try{

            doQuery( client, "IBM", 0 );

            Collection<?> listKieObjects = doQuery( client, "RHT", 1 );
            StockTickEvent event = (StockTickEvent)listKieObjects.iterator().next();
            assertTrue(event.getCompany().equals("RHT"));
        }finally {
            client.close();
        }
    }

    private Collection<?> doQuery( RemoteStreamingKieSessionImpl client, String stockName, int expectedResult) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        CompletableFuture<Collection> listKieObjectsFuture;
        Collection listKieObjects;
        listKieObjectsFuture = client.getObjects("stockTickEventQuery" , "stock", stockName);
        listKieObjects = listKieObjectsFuture.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResult, listKieObjects.size());
        return listKieObjects;
    }

}
