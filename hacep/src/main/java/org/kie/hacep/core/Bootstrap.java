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
package org.kie.hacep.core;

import java.util.Arrays;

import org.kie.remote.Config;
import org.kie.remote.EnvConfig;
import org.kie.hacep.core.infra.consumer.ConsumerController;
import org.kie.hacep.core.infra.election.LeaderElection;
import org.kie.hacep.core.infra.election.State;
import org.kie.remote.impl.producer.EventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Here is where we start all the services needed by the POD
 */
public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static EventProducer<?> eventProducer;
    private static ConsumerController consumerController;
    private static CoreKube coreKube;

    public static void startEngine(EnvConfig envConfig) {
        startEngine(envConfig, null);
    }

    public static void startEngine(EnvConfig envConfig, State initialState) {
        //order matter
        coreKube = new CoreKube(envConfig.getNamespace(), initialState);
        startProducer();
        startConsumers(envConfig);
        leaderElection();
        logger.info("CONFIGURE on start engine:{}", Config.getDefaultConfig());
    }

    public static void stopEngine() {
        logger.info("Stop engine");

        LeaderElection leadership = coreKube.getLeaderElection();
        try {
            leadership.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(),
                         e);
        }
        if ( consumerController != null) {
            consumerController.stop();
        }
        if (eventProducer != null) {
            eventProducer.stop();
        }
        eventProducer = null;
        consumerController = null;
    }

    // only for tests
    public static ConsumerController getConsumerController(){
        return consumerController;
    }

    private static void leaderElection() {
        //KubernetesLockConfiguration configuration = CoreKube.getKubernetesLockConfiguration();
        //@TODO configure from env the namespace
        //KubernetesClient client = Core.getKubeClient();
        //client.events().inNamespace("my-kafka-project").watch(WatcherFactory.createModifiedLogWatcher(configuration.getPodName()));
        LeaderElection leadership = coreKube.getLeaderElection();
        coreKube.getLeaderElection().addCallbacks(Arrays.asList( consumerController.getCallback() ));
        try {
            leadership.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void startProducer() {
        eventProducer = new EventProducer<>();
        eventProducer.start(Config.getProducerConfig("EventProducer"));
    }

    private static void startConsumers(EnvConfig envConfig) {
        consumerController = new ConsumerController(envConfig, eventProducer);
        consumerController.start();
    }



}
