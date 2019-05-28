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

import org.junit.After;
import org.junit.Before;
import org.kie.KafkaUtilTest;
import org.kie.u212.core.Bootstrap;
import org.kie.u212.core.infra.election.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaTest {

    private KafkaUtilTest kafkaServerTest;
    private Logger kafkaLogger = LoggerFactory.getLogger("org.kie.u212.kafkaLogger");
    private final  String TEST_KAFKA_LOGGER_TOPIC = "testevents";
    private final  String TEST_TOPIC = "test";


    @Before
    public  void setUp() throws Exception{
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.createTopic(TEST_TOPIC);
        kafkaServerTest.createTopic(Config.EVENTS_TOPIC);
        kafkaServerTest.createTopic(Config.CONTROL_TOPIC);
        Bootstrap.startEngine();
        Bootstrap.getRestarter().getCallback().updateStatus(State.NOT_LEADER);
    }

    @After
    public  void tearDown(){
        try {
            Bootstrap.stopEngine();
        }catch (ConcurrentModificationException ex){ }
        kafkaServerTest.deleteTopic(TEST_TOPIC);
        kafkaServerTest.deleteTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.deleteTopic(Config.EVENTS_TOPIC);
        kafkaServerTest.deleteTopic(Config.CONTROL_TOPIC);
        kafkaServerTest.shutdownServer();
    }

}
