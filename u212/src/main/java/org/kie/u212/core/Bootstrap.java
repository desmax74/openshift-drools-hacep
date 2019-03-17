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

import java.util.Arrays;

import org.kie.u212.consumer.DroolsConsumer;
import org.kie.u212.consumer.DroolsConsumerController;
import org.kie.u212.consumer.EmptyConsumerHandler;
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
    private static DroolsConsumerController consumerController ;
    private static EventProducer<StockTickEvent> eventProducer ;
    private static DroolsConsumer<StockTickEvent> eventConsumer ;

    public static void startEngine(){
        //order matter
        leaderElection();
        startProducer();
        startConsumer();
        addCallbacks();
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

    public void startNewThreadConsumer(){
        consumerController.consumeEvents();
    }


    private static void leaderElection() {
        KubernetesLockConfiguration configuration = Core.getKubernetesLockConfiguration();
        logger.info("ServletContextInitialized on pod:{}", configuration.getPodName());
        //@TODO configure from env the namespace
        //KubernetesClient client = Core.getKubeClient();
        //client.events().inNamespace("my-kafka-project").watch(WatcherFactory.createModifiedLogWatcher(configuration.getPodName()));
        LeaderElection leadership = Core.getLeaderElection();
        try {
            leadership.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    private static void startProducer(){
        eventProducer = new EventProducer<>();
        if(Core.getLeaderElection().amITheLeader()) {
            eventProducer.start(Config.getDefaultConfig());
        }
    }


    private static void startConsumer(){
        eventConsumer = new DroolsConsumer<>(Core.getKubernetesLockConfiguration().getPodName());
        //eventConsumer.start(new DroolsConsumerHandler());
        eventConsumer.start(new EmptyConsumerHandler());
        consumerController = new DroolsConsumerController(eventConsumer);
        consumerController.consumeEvents();
        if(logger.isInfoEnabled()) {
            logger.info("Start consumer on Group:{} , duration:{} pollSize:{}",
                        Config.GROUP,
                        Config.LOOP_DURATION,
                        Config.DEFAULT_POLL_SIZE);
        }
    }


    private static void addCallbacks() {
        Core.getLeaderElection().addCallbacks(Arrays.asList(eventConsumer,eventProducer));
    }
}
