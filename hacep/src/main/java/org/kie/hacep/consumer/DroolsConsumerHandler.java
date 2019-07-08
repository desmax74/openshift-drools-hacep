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
package org.kie.hacep.consumer;

import java.util.Queue;

import org.kie.api.time.SessionPseudoClock;
import org.kie.hacep.ConverterUtil;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.KieSessionHolder;
import org.kie.hacep.core.infra.SessionSnapShooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.EventConsumer;
import org.kie.hacep.core.infra.consumer.ItemToProcess;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.producer.EventProducer;
import org.kie.hacep.core.infra.producer.Producer;
import org.kie.hacep.model.ControlMessage;
import org.kie.remote.RemoteCommand;
import org.kie.remote.command.VisitableCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsConsumerHandler implements ConsumerHandler {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private SessionPseudoClock clock;
    private Producer producer;
    private SessionSnapShooter snapshooter;
    private SnapshotInfos snapshotInfos;
    private EnvConfig config;
    private KieSessionHolder kieSessionHolder;
    private CommandHandler commandHandler;

    public DroolsConsumerHandler(EventProducer producer, SessionSnapShooter snapshooter, EnvConfig config, KieSessionHolder kieSessionHolder) {
        this.kieSessionHolder = kieSessionHolder;
        this.config = config;
        this.snapshooter = snapshooter;
        clock = kieSessionHolder.getKieSession().getSessionClock();
        this.producer = producer;
        commandHandler = new CommandHandler(kieSessionHolder, config, producer);
    }

    public SessionSnapShooter getSnapshooter(){
        return snapshooter;
    }

    public void process(ItemToProcess item, State state, EventConsumer consumer, Queue<Object> sideEffects) {
        RemoteCommand command  = ConverterUtil.deSerializeObjInto((byte[])item.getObject(), RemoteCommand.class);
        processCommand( command, state );

        if (state.equals(State.LEADER)) {
            Queue<Object> results = DroolsExecutor.getInstance().getAndReset();
            ControlMessage newControlMessage = new ControlMessage(command.getId(), results);
            producer.produceSync(config.getControlTopicName(), command.getId(), newControlMessage);
        }
    }


    public void processWithSnapshot(ItemToProcess item,
                                    State currentState,
                                    EventConsumer consumer,
                                    Queue<Object> sideEffects) {
        logger.info("SNAPSHOT !!!");
        // TODO add fhMap to snapshot image
        snapshooter.serialize(kieSessionHolder, item.getKey(), item.getOffset());
        process(item, currentState, consumer, sideEffects);
    }

    private void processCommand( RemoteCommand command, State state ) {
        boolean execute = state.equals(State.LEADER) || command.isPermittedForReplicas();
        VisitableCommand visitable = (VisitableCommand) command;
        if (execute) {
            visitable.accept(commandHandler);
        }
    }



}