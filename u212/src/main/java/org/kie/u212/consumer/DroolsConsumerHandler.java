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
package org.kie.u212.consumer;

import java.util.Queue;

import org.kie.api.KieServices;
import org.kie.api.event.rule.DefaultRuleRuntimeEventListener;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionPseudoClock;
import org.kie.remote.RemoteCommand;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.VisitableCommand;
import org.kie.u212.ConverterUtil;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.KieSessionHolder;
import org.kie.u212.core.infra.SessionSnapShooter;
import org.kie.u212.core.infra.SnapshotInfos;
import org.kie.u212.core.infra.consumer.ConsumerHandler;
import org.kie.u212.core.infra.consumer.EventConsumer;
import org.kie.u212.core.infra.consumer.ItemToProcess;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.producer.Producer;
import org.kie.u212.model.ControlMessage;
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
    private BidirectionalMap<RemoteFactHandle, FactHandle> fhMap = new BidirectionalMap<>();
    private CommandHandler commandHandler;

    public DroolsConsumerHandler(EventProducer producer, SessionSnapShooter snapshooter, EnvConfig config, KieSessionHolder kieSessionHolder) {
        this.kieSessionHolder = kieSessionHolder;
        this.config = config;
        this.snapshooter = snapshooter;
        KieServices srv = KieServices.get();
        if (srv != null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            logger.info("Creating new Kie Session");
            kieSessionHolder.replaceKieSession(initKieSession( kieContainer.newKieSession()));
            clock = kieSessionHolder.getKieSession().getSessionClock();
            this.producer = producer;
        } else {
            logger.error("KieService is null");
        }
        commandHandler = new CommandHandler(fhMap, kieSessionHolder, config, producer);
    }

    public DroolsConsumerHandler(EventProducer producer, SessionSnapShooter snapshooter, SnapshotInfos infos, EnvConfig config, KieSessionHolder kieSessionHolder) {
        this.config = config;
        this.snapshotInfos = infos;
        this.snapshooter = snapshooter;
        if (snapshotInfos.getKieSession() == null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            kieSessionHolder.replaceKieSession(initKieSession( kieContainer.newKieSession()));
        } else {
            logger.info("Applying snapshot");
            kieSessionHolder.replaceKieSession(initKieSession( infos.getKieSession()));
        }
        clock = kieSessionHolder.getKieSession().getSessionClock();
        this.producer = producer;
    }

    private KieSession initKieSession(KieSession kieSession) {
        kieSession.addEventListener( new DefaultRuleRuntimeEventListener() {
            @Override
            public void objectDeleted( ObjectDeletedEvent objectDeletedEvent ) {
                fhMap.removeValue( objectDeletedEvent.getFactHandle() );
            }
        } );
        return kieSession;
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
        snapshooter.serialize(kieSessionHolder.getKieSession(), fhMap, item.getKey(), item.getOffset());
        process(item, currentState, consumer, sideEffects);
    }

    private void processCommand( RemoteCommand command, State state ) {
        boolean execute = state.equals(State.LEADER) || command.isPermittedForReplicas();
        VisitableCommand visitable = (VisitableCommand) command;
        visitable.accept(commandHandler, execute);
    }



}