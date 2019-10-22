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
package org.kie.hacep.core.infra.consumer;

import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.DefaultSessionSnapShooter;

import org.kie.hacep.core.infra.election.State;

import org.kie.remote.DroolsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultEventConsumerLifecycle implements EventConsumerLifecycle {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaConsumer.class);
    private EventConsumerStatus status;
    private DroolsConsumerHandler consumerHandler;
    private EnvConfig config;
    private KafkaConsumers kafkaConsumers;

    public DefaultEventConsumerLifecycle(DroolsConsumerHandler consumerHandler, EnvConfig config, DefaultSessionSnapShooter snapShooter){
        status = new EventConsumerStatus();
        this.consumerHandler = consumerHandler;
        this.config = config;
        kafkaConsumers = new KafkaConsumers(status, config,this, this.consumerHandler, this.consumerHandler.getSnapshooter());
    }

    public KafkaConsumers getConsumers(){
        return  kafkaConsumers;
    }

    public EventConsumerStatus getStatus(){
        return status;
    }

    public void askAndProcessSnapshotOnDemand() {
        status.setAskedSnapshotOnDemand(true);
        boolean completed = consumerHandler.initializeKieSessionFromSnapshotOnDemand(config);
        if (logger.isInfoEnabled()) {
            logger.info("askAndProcessSnapshotOnDemand:{}", completed);
        }
        if (!completed) {
            throw new RuntimeException("Can't obtain a snapshot on demand");
        }
    }

    public  void updateOnRunningConsumer(State state) {
        logger.info("updateOnRunning Consumer");
        if (state.equals(State.LEADER) ) {
            DroolsExecutor.setAsLeader();
            kafkaConsumers.restart(state);
        } else if (state.equals(State.REPLICA)) {
            DroolsExecutor.setAsReplica();
            kafkaConsumers.restart(state);
        }
    }

    @Override
    public void enableConsumeAndStartLoop(State state) {
        kafkaConsumers.enableConsumeAndStartLoop(state);
    }

    @Override
    public void stopConsume() {
        kafkaConsumers.stop();
    }
}
