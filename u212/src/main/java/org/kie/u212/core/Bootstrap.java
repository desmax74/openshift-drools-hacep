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
import java.util.UUID;

import org.kie.u212.Config;
import org.kie.u212.EnvConfig;
import org.kie.u212.consumer.DroolsConsumerHandler;
import org.kie.u212.core.infra.SessionSnapShooter;
import org.kie.u212.core.infra.SnapshotInfos;
import org.kie.u212.core.infra.consumer.ConsumerController;
import org.kie.u212.core.infra.consumer.Restarter;
import org.kie.u212.core.infra.election.LeaderElection;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.utils.Printer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Here is where we start all the services needed by the POD
 */
public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static ConsumerController consumerController;
    private static EventProducer<?> eventProducer;
    private static Restarter restarter;
    private static SessionSnapShooter snaptshooter;
    private static CoreKube coreKube;

    public static void startEngine(Printer printer, EnvConfig envConfig) {
        //order matter
        coreKube = new CoreKube(envConfig.getNamespace());
        leaderElection();
        startProducer(envConfig);
        startConsumers(printer, envConfig);
        addMasterElectionCallbacks();
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

        if (restarter != null) {
            restarter.getConsumer().stop();
        }
        if (eventProducer != null) {
            eventProducer.stop();
        }
        snaptshooter.close();
        consumerController.stopConsumeEvents();
        consumerController = null;
        eventProducer = null;
        restarter = null;
        snaptshooter = null;
    }

    public static Restarter getRestarter(){
        return restarter;
    }


    private static void leaderElection() {
        //KubernetesLockConfiguration configuration = CoreKube.getKubernetesLockConfiguration();
        //@TODO configure from env the namespace
        //KubernetesClient client = Core.getKubeClient();
        //client.events().inNamespace("my-kafka-project").watch(WatcherFactory.createModifiedLogWatcher(configuration.getPodName()));
        LeaderElection leadership = coreKube.getLeaderElection();
        try {
            leadership.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void startProducer(EnvConfig envConfig) {
        eventProducer = new EventProducer<>();
        eventProducer.start(Config.getProducerConfig());
    }

    private static void startConsumers(Printer printer,
                                       EnvConfig envConfig) {
        snaptshooter = new SessionSnapShooter(envConfig);
        SnapshotInfos infos = snaptshooter.deserializeEventWrapper();
        restarter = new Restarter(printer);
        restarter.createDroolsConsumer(envConfig);
        logger.info("start consumer with:{}", infos);
        if (infos.getKeyDuringSnaphot() != null) {
            restarter.getConsumer().createConsumer(new DroolsConsumerHandler(eventProducer, snaptshooter, infos, envConfig), infos);
        } else {
            restarter.getConsumer().createConsumer(new DroolsConsumerHandler(eventProducer, snaptshooter, envConfig));
        }
        consumerController = new ConsumerController(restarter);
        consumerController.consumeEvents();
    }

    private static void addMasterElectionCallbacks() {
        coreKube.getLeaderElection().addCallbacks(Arrays.asList(restarter.getCallback(), eventProducer));
    }

}
