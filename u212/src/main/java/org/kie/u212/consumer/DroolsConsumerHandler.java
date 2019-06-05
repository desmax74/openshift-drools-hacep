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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kie.api.KieServices;
import org.kie.api.event.rule.DefaultRuleRuntimeEventListener;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionPseudoClock;
import org.kie.remote.RemoteCommand;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.DeleteCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.infra.SessionSnapShooter;
import org.kie.u212.core.infra.SnapshotInfos;
import org.kie.u212.core.infra.consumer.ConsumerHandler;
import org.kie.u212.core.infra.consumer.EventConsumer;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.producer.Producer;
import org.kie.u212.model.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsConsumerHandler implements ConsumerHandler {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private KieSession kieSession;
    private SessionPseudoClock clock;
    private Producer producer;
    private SessionSnapShooter snapshooter;
    private SnapshotInfos snapshotInfos;
    private EnvConfig config;

    private BidirectionalMap<RemoteFactHandle, FactHandle> fhMap = new BidirectionalMap<>();

    public DroolsConsumerHandler(EventProducer producer, SessionSnapShooter snapshooter, EnvConfig config) {
        this.config = config;
        this.snapshooter = snapshooter;
        KieServices srv = KieServices.get();
        if (srv != null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            logger.info("Creating new Kie Session");
            kieSession = initKieSession( kieContainer.newKieSession() );
            clock = kieSession.getSessionClock();
            this.producer = producer;
        } else {
            logger.error("KieService is null");
        }
    }

    public DroolsConsumerHandler(EventProducer producer, SessionSnapShooter snapshooter, SnapshotInfos infos, EnvConfig config) {
        this.config = config;
        this.snapshotInfos = infos;
        this.snapshooter = snapshooter;
        if (snapshotInfos.getKieSession() == null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            kieSession = initKieSession( kieContainer.newKieSession() );
        } else {
            logger.info("Applying snapshot");
            kieSession = initKieSession( infos.getKieSession() );
        }
        clock = kieSession.getSessionClock();
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

    @Override
    public void process(ConsumerRecord record, State state, EventConsumer consumer, Queue<Object> sideEffects) {
        RemoteCommand command = (RemoteCommand) record.value();
        processCommand( command );
        kieSession.fireAllRules();

        if (state.equals(State.LEADER)) {
            Queue<Object> results = DroolsExecutor.getInstance().getAndReset();
            ControlMessage newControlMessage = new ControlMessage(command.getId(), results);
            producer.produceSync(new ProducerRecord<>(config.getControlTopicName(), command.getId(), newControlMessage ));
        }
    }

    private void processCommand( RemoteCommand command ) {

        if (command instanceof InsertCommand ) {
            InsertCommand insert = ( InsertCommand ) command;
            RemoteFactHandle remoteFH = insert.getFactHandle();
            FactHandle fh = kieSession.getEntryPoint( insert.getEntryPoint() ).insert( remoteFH.getObject() );
            fhMap.put( remoteFH, fh );
        } else if (command instanceof DeleteCommand ) {
            DeleteCommand delete = ( DeleteCommand ) command;
            RemoteFactHandle remoteFH = delete.getFactHandle();
            kieSession.getEntryPoint( delete.getEntryPoint() ).delete( fhMap.get(remoteFH) );
        } else {
            throw new UnsupportedOperationException( "Unkonwn command: " + command );
        }
    }

    @Override
    public void processWithSnapshot(ConsumerRecord record,
                                    State currentState,
                                    EventConsumer consumer,
                                    Queue<Object> sideEffects) {
        logger.info("SNAPSHOT !!!");
        // TODO add fhMap to snapshot image
        snapshooter.serialize(kieSession, fhMap, record.key().toString(), record.offset());
        process(record, currentState, consumer, sideEffects);
    }

    public SessionSnapShooter getSnapshooter(){
        return snapshooter;
    }
}
