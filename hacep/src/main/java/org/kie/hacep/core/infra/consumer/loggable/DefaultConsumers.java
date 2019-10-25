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
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.Consumers;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.ConsumerUtils;
import org.kie.hacep.message.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConsumers<T> implements Consumers<T> {

    private Logger logger = LoggerFactory.getLogger(DefaultConsumers.class);
    private EnvConfig envConfig;
    private EventConsumerStatus status;
    private CoreConsumer coreConsumer;

    public DefaultConsumers(EventConsumerStatus status,
                            EnvConfig envConfig,
                            ConsumerHandler consumerHandler,
                            SessionSnapshooter snapShooter) {
        this.envConfig = envConfig;
        this.status = status;
        if(envConfig.isUnderTest()) {
            this.coreConsumer = (CoreConsumer) LoggableInvocationHandler.createProxy(
                    new DefaultKafkaCoreConsumer(consumerHandler, envConfig, status, snapShooter));
        }else{
            this.coreConsumer = new DefaultKafkaCoreConsumer(consumerHandler, envConfig, status, snapShooter);
        }
    }

    @Override
    public void initConsumer() {
        coreConsumer.initConsumer();
    }

    @Override
    public void stop() {
        coreConsumer.stop();
    }

    @Override
    public void poll() {
        coreConsumer.poll();
    }


    @Override
    public void enableConsumeAndStartLoop(State state) {
        coreConsumer.enableConsumeAndStartLoop(state);
        setLastProcessedKey();
        coreConsumer.assignAndStartConsume();
    }

    @Override
    public void setLastProcessedKey() {
        ControlMessage lastControlMessage = ConsumerUtils.getLastEvent(envConfig.getControlTopicName(),
                                                                       envConfig.getPollTimeout());
        settingsOnAEmptyControlTopic(lastControlMessage);
        status.setProcessingKey(lastControlMessage.getId());
        status.setProcessingKeyOffset(lastControlMessage.getOffset());
    }

    @Override
    public void settingsOnAEmptyControlTopic(ControlMessage lastWrapper) {
        if (lastWrapper.getId() == null) {// completely empty or restart of ephemeral already used
            if (status.getCurrentState().equals(State.REPLICA)) {
                coreConsumer.pollControl();
            }
        }
    }

    @Override
    public void internalRestartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        coreConsumer.restartConsumer();
    }

    @Override
    public void restart(State state) {
        coreConsumer.stopConsume();
        internalRestartConsumer();
        enableConsumeAndStartLoop(state);
    }
}