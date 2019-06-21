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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.remote.RemoteCepKieSession;
import org.kie.u212.core.Bootstrap;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.utils.PrinterLogImpl;
import org.kie.u212.model.FactCountMessage;
import org.kie.u212.producer.RemoteCepKieSessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class RemoteCepKieSessionImplTest {

    private KafkaUtilTest kafkaServerTest;
    private Logger logger = LoggerFactory.getLogger(RemoteCepKieSessionImplTest.class);
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
        try (RemoteCepKieSessionImpl client = new RemoteCepKieSessionImpl(Config.getProducerConfig(),
                                                                          config)) {
            client.listen();
            CompletableFuture<Long> factCountCallBack = new CompletableFuture<>();
            client.getFactCount(factCountCallBack);
            Object callbackValue = factCountCallBack.get(15,
                                                         TimeUnit.SECONDS);
            FactCountMessage msg = (FactCountMessage) callbackValue;
            assertTrue(msg.getFactCount() == 7);
        }
    }

}
