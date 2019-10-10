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
import org.kie.hacep.core.infra.election.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The default consumer relies on the Consumer thread and
 * is based on the loop around poll method.
 */
public class DefaultKafkaConsumerWithProxy<T> implements EventConsumer {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaConsumer.class);
    private DroolsConsumerHandler consumerHandler;
    private EnvConfig config;
    private KafkaConsumers kafkaConsumers;
    private EventConsumerLifecycleProxy proxy ;


    public DefaultKafkaConsumerWithProxy(EnvConfig config) {
        this.config = config;
    }

    public void initConsumer(ConsumerHandler consumerHandler) {
        this.consumerHandler = (DroolsConsumerHandler) consumerHandler;
        proxy = new DefaultEventConsumerLifecycleProxy(this.consumerHandler, this.config, this.consumerHandler.getSnapshooter());
        kafkaConsumers = proxy.getConsumers();
        kafkaConsumers.initConsumer();
    }

    @Override
    public void stop() {
        proxy.stopConsume();
        kafkaConsumers.stop();
        proxy.getStatus().setExit(true);
        consumerHandler.stop();
    }

    @Override
    public void updateStatus(State state) {
        boolean changedState = !state.equals(proxy.getStatus().getCurrentState());
        if(proxy.getStatus().getCurrentState() == null ||  changedState){
            proxy.getStatus().setCurrentState(state);
        }
        if (proxy.getStatus().isStarted() && changedState && !proxy.getStatus().getCurrentState().equals(State.BECOMING_LEADER)) {
            proxy.updateOnRunningConsumer(state);
        } else if(!proxy.getStatus().isStarted()) {
            if (state.equals(State.REPLICA)) {
                //ask and wait a snapshot before start
                if (!config.isSkipOnDemanSnapshot() && !proxy.getStatus().isAskedSnapshotOnDemand()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("askAndProcessSnapshotOnDemand:");
                    }
                    proxy.askAndProcessSnapshotOnDemand();
                }
            }
            //State.BECOMING_LEADER won't start the pod
            if (state.equals(State.LEADER) || state.equals(State.REPLICA)) {
                if (logger.isInfoEnabled()) {
                    logger.info("enableConsumeAndStartLoop:{}", state);
                }
                proxy.enableConsumeAndStartLoop(state);
            }
        }
    }

    public void poll(int durationMillis) {
        kafkaConsumers.poll(durationMillis);
    }
}