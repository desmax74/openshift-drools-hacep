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
import java.util.Properties;

import org.kie.u212.Config;
import org.kie.u212.consumer.DroolsConsumerHandler;
import org.kie.u212.core.infra.consumer.ConsumerController;
import org.kie.u212.core.infra.consumer.Restarter;
import org.kie.u212.core.infra.election.KubernetesLockConfiguration;
import org.kie.u212.core.infra.election.LeaderElection;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Here is where we start all the services needed by the POD
 */
public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static ConsumerController consumerController;
    private static EventProducer<StockTickEvent> eventProducer;
    private static Restarter restarter;

    public static void startEngine() {
        //order matter
        leaderElection();
        Properties properties = Config.getDefaultConfig();
        properties.put("group.id",
                       Core.getKubernetesLockConfiguration().getPodName().
                               replace("openshift-kie-", ""));
        startProducer(properties);
        startConsumer(properties);
        addCallbacks();
        ConsumerUtils.printOffset(Config.EVENTS_TOPIC, properties);
        ConsumerUtils.printOffset(Config.CONTROL_TOPIC, properties);
        logger.info("CONFIGURE ON START ENGINE:{}", properties);
    }

    public static void stopEngine() {
        LeaderElection leadership = Core.getLeaderElection();
        try {
            leadership.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(),
                         e);
        }
        if (restarter != null) {
            restarter.getConsumer().stop();
        }
        if (eventProducer != null) {
            eventProducer.stop();
        }
    }

    private static void leaderElection() {
        KubernetesLockConfiguration configuration = Core.getKubernetesLockConfiguration();
        logger.info("ServletContextInitialized on pod:{}",
                    configuration.getPodName());
        //@TODO configure from env the namespace
        //KubernetesClient client = Core.getKubeClient();
        //client.events().inNamespace("my-kafka-project").watch(WatcherFactory.createModifiedLogWatcher(configuration.getPodName()));
        LeaderElection leadership = Core.getLeaderElection();
        try {
            leadership.start();
        } catch (Exception e) {
            logger.error(e.getMessage(),
                         e);
        }
    }

    private static void startProducer(Properties properties) {
        eventProducer = new EventProducer<>();
        eventProducer.start(properties);
    }

    private static void startConsumer(Properties properties) {
        restarter = new Restarter(properties);
        restarter.createDroolsConsumer(Core.getKubernetesLockConfiguration().getPodName());
        restarter.getConsumer().createConsumer(new DroolsConsumerHandler(eventProducer), properties); //@TODO
        consumerController = new ConsumerController(restarter);
        consumerController.consumeEvents();
    }

    private static void addCallbacks() {
        Core.getLeaderElection().addCallbacks(Arrays.asList(restarter.getCallback(),
                                                            eventProducer));
    }
}
