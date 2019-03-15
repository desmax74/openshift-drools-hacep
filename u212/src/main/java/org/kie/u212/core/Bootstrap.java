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
package org.kie.u212.core;

import io.fabric8.kubernetes.client.KubernetesClient;

import org.kie.u212.consumer.DroolsConsumer;
import org.kie.u212.consumer.DroolsConsumerController;
import org.kie.u212.consumer.DroolsConsumerHandler;
import org.kie.u212.election.KubernetesLockConfiguration;

import org.kie.u212.election.LeaderElection;
import org.kie.u212.infra.producer.EventProducer;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Here is where we start all the services needed by the POD
 *
 * */
public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static DroolsConsumerController consumerController;
    private static final EventProducer<StockTickEvent> eventProducer = new EventProducer<>();
    private static DroolsConsumer<StockTickEvent> eventConsumer ;

    public static void startEngine(){
        //order matter
        leaderElection();
        startProducer();
        startConsumer();
    }

    public static void stopEngine(){
        LeaderElection leadership = Core.getLeaderElection();
        try {
            leadership.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(),
                         e);
        }
        eventConsumer.stop();
        eventProducer.stop();
    }



    private static void leaderElection() {
        KubernetesLockConfiguration configuration = Core.getKubernetesLockConfiguration();
        logger.info("ServletContextInitialized on pod:{}", configuration.getPodName());
        KubernetesClient client = Core.getKubeClient();
        //@TODO configure from env the namespace
        client.events().inNamespace("my-kafka-project").watch(WatcherFactory.createModifiedLogWatcher(configuration.getPodName()));
        LeaderElection leadership = Core.getLeaderElection();
        try {
            leadership.start();
        } catch (Exception e) {
            logger.error(e.getMessage(),
                         e);
        }
    }

    private static void startConsumer(){
        eventConsumer = new DroolsConsumer<>(Core.getKubernetesLockConfiguration().getPodName(), new DroolsConsumerHandler());
        eventConsumer.start();
        consumerController = new DroolsConsumerController(eventConsumer);
        consumerController.consumeEvents((Core.getLeaderElection().amITheLeader() ? Config.MASTER_TOPIC : Config.USERS_INPUT_TOPIC),Config.GROUP, Config.LOOP_DURATION, Config.DEFAULT_POLL_SIZE);
        logger.info("Start consumer on Group:{} , duration:{} pollSize:{}", Config.GROUP, Config.LOOP_DURATION , Config.DEFAULT_POLL_SIZE);
    }



    private static void startProducer(){
        if(Core.getLeaderElection().amITheLeader()) {
            eventProducer.start(Config.getDefaultConfig());
        }
    }
}
