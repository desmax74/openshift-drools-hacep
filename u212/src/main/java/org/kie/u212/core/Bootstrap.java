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

import org.kie.u212.Config;
import org.kie.u212.consumer.DroolsConsumerHandler;
import org.kie.u212.core.infra.consumer.Restarter;
import org.kie.u212.core.infra.consumer.ConsumerController;
import org.kie.u212.consumer.EmptyConsumerHandler;
import org.kie.u212.core.infra.election.KubernetesLockConfiguration;

import org.kie.u212.core.infra.election.LeaderElection;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Here is where we start all the services needed by the POD
 *
 * */
public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static ConsumerController consumerController ;
    private static EventProducer<StockTickEvent> eventProducer ;
    private static Restarter restarter;

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
        restarter.getConsumer().stop();
        eventProducer.stop();
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
        eventProducer.start(Config.getDefaultConfig());
    }


    private static void startConsumer(){
        restarter = new Restarter();
        restarter.createDroolsConsumer(Core.getKubernetesLockConfiguration().getPodName());
        //restarter.getConsumer().start(new EmptyConsumerHandler());
        restarter.getConsumer().start(new DroolsConsumerHandler(eventProducer));
        consumerController = new ConsumerController(restarter);
        consumerController.consumeEvents();
        if(logger.isInfoEnabled()) {
            logger.info("Start consumer on Group:{} , duration:{} pollSize:{} aurtocommit:{}",
                        Config.GROUP,
                        Config.LOOP_DURATION,
                        Config.DEFAULT_POLL_SIZE,
                        Config.DEFAULT_COMMIT_SYNC);
        }
    }


    private static void addCallbacks() {
        Core.getLeaderElection().addCallbacks(Arrays.asList(restarter.getCallback(), eventProducer));
    }
}