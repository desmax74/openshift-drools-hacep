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
package org.kie.hacep.core.infra.consumer.loggable;

import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.Consumers;
import org.kie.hacep.core.infra.consumer.EventConsumer;
import org.kie.hacep.core.infra.election.State;
import org.kie.remote.DroolsExecutor;
import org.kie.remote.impl.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default consumer relies on the Consumer thread and
 * is based on the loop around poll method.
 */
public class LoggableDefaultKafkaConsumer<T> implements EventConsumer {

    private Logger logger = LoggerFactory.getLogger(LoggableDefaultKafkaConsumer.class);
    private ConsumerHandler consumerHandler;
    private EnvConfig envConfig;
    private Consumers kafkaConsumers;
    private EventConsumerStatus status;

    public LoggableDefaultKafkaConsumer(EnvConfig envConfig) {
        this.envConfig = envConfig;
        this.status = getConsumerStatus();
    }

    @Override
    public void initConsumer(Producer producer) {
        this.consumerHandler = getDroolsConsumerHandler(producer);
        kafkaConsumers = getConsumersImpl();
        kafkaConsumers.initConsumer();
    }

    @Override
    public void stop() {
        stopConsume();
        kafkaConsumers.stop();
        status.setExit(true);
        consumerHandler.stop();
    }

    public void enableConsumeAndStartLoop(State state) {
        kafkaConsumers.enableConsumeAndStartLoop(state);
    }

    public void stopConsume() {
        kafkaConsumers.stop();
    }

    @Override
    public void poll() {
        kafkaConsumers.poll();
    }

    @Override
    public void updateStatus(State state) {
        boolean changedState = !state.equals(status.getCurrentState());
        if (status.getCurrentState() == null || changedState) {
            status.setCurrentState(state);
        }
        if (status.isStarted() && changedState && !status.getCurrentState().equals(State.BECOMING_LEADER)) {
            updateOnRunningConsumer(state);
        } else if (!status.isStarted()) {
            if (state.equals(State.REPLICA)) {
                //ask and wait a snapshot before start
                if (!envConfig.isSkipOnDemanSnapshot() && !status.isAskedSnapshotOnDemand()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("askAndProcessSnapshotOnDemand:");
                    }
                    askAndProcessSnapshotOnDemand();
                }
            }
            //State.BECOMING_LEADER won't start the pod
            if (state.equals(State.LEADER) || state.equals(State.REPLICA)) {
                if (logger.isInfoEnabled()) {
                    logger.info("enableConsumeAndStartLoop:{}",
                                state);
                }
                enableConsumeAndStartLoop(state);
            }
        }
    }

    public void askAndProcessSnapshotOnDemand() {
        status.setAskedSnapshotOnDemand(true);
        boolean completed = ((DroolsConsumerHandler) consumerHandler).initializeKieSessionFromSnapshotOnDemand(envConfig);
        if (logger.isInfoEnabled()) {
            logger.info("askAndProcessSnapshotOnDemand:{}",
                        completed);
        }
        if (!completed) {
            throw new RuntimeException("Can't obtain a snapshot on demand");
        }
    }

    public void updateOnRunningConsumer(State state) {
        if (state.equals(State.LEADER)) {
            DroolsExecutor.setAsLeader();
            kafkaConsumers.restart(state);
        } else if (state.equals(State.REPLICA)) {
            DroolsExecutor.setAsReplica();
            kafkaConsumers.restart(state);
        }
    }

    //LoggableProxy
    private ConsumerHandler getDroolsConsumerHandler(Producer producer) {
        ConsumerHandler handler;
        if (envConfig.isUnderTest()) {
            handler = (ConsumerHandler) LoggableInvocationHandler.createProxy(new DroolsConsumerHandler(producer,
                                                                                                        envConfig));
        } else {
            handler = new DroolsConsumerHandler(producer,
                                                envConfig);
        }
        return handler;
    }

    private EventConsumerStatus getConsumerStatus() {
        EventConsumerStatus status;
        if (envConfig.isUnderTest()) {
            status = (EventConsumerStatus) LoggableInvocationHandler.createProxy(new DefaultEventConsumerStatus());
        } else {
            status = new DefaultEventConsumerStatus();
        }
        return status;
    }

    private Consumers getConsumersImpl() {
        Consumers consumers;
        if (envConfig.isUnderTest()) {
            consumers = (Consumers) LoggableInvocationHandler.createProxy(new DefaultConsumers(status,
                                                                                               envConfig,
                                                                                               this.consumerHandler,
                                                                                               this.consumerHandler.getSnapshooter()));
        } else {
            consumers = new DefaultConsumers(status,
                                             envConfig,
                                             this.consumerHandler,
                                             this.consumerHandler.getSnapshooter());
        }
        return consumers;
    }
}