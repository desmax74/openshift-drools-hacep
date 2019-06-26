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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.ListObjectsCommand;
import org.kie.remote.command.UpdateCommand;
import org.kie.remote.command.VisitableCommand;
import org.kie.remote.command.VisitorCommand;
import org.kie.u212.ConverterUtil;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.KieSessionHolder;
import org.kie.u212.core.infra.SessionSnapShooter;
import org.kie.u212.core.infra.SnapshotInfos;
import org.kie.u212.core.infra.consumer.ConsumerHandler;
import org.kie.u212.core.infra.consumer.EventConsumer;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.producer.Producer;
import org.kie.u212.model.ControlMessage;
import org.kie.u212.model.FactCountMessage;
import org.kie.u212.model.ListKieSessionObjectMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsConsumerHandler implements ConsumerHandler,
                                              VisitorCommand {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private SessionPseudoClock clock;
    private Producer producer;
    private SessionSnapShooter snapshooter;
    private SnapshotInfos snapshotInfos;
    private EnvConfig config;
    private KieSessionHolder kieSessionHolder;

    private BidirectionalMap<RemoteFactHandle, FactHandle> fhMap = new BidirectionalMap<>();

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

    @Override
    public void process(ConsumerRecord record, State state, EventConsumer consumer, Queue<Object> sideEffects) {
        RemoteCommand command  = ConverterUtil.deSerializeObjInto((byte[])record.value(), RemoteCommand.class);
        processCommand( command, state );

        if (state.equals(State.LEADER)) {
            Queue<Object> results = DroolsExecutor.getInstance().getAndReset();
            ControlMessage newControlMessage = new ControlMessage(command.getId(), results);
            producer.produceSync(config.getControlTopicName(), command.getId(), newControlMessage);
        }
    }

    private void processCommand( RemoteCommand command, State state ) {
        boolean execute = state.equals(State.LEADER) || command.isPermittedForReplicas();
        VisitableCommand visitable = (VisitableCommand) command;
        visitable.accept(this, execute);
    }

    @Override
    public void visit(InsertCommand command, boolean execute) {
        if(execute) {
            RemoteFactHandle remoteFH = command.getFactHandle();
            FactHandle fh = kieSessionHolder.getKieSession().getEntryPoint(command.getEntryPoint()).insert(remoteFH.getObject());
            fhMap.put(remoteFH, fh);
            kieSessionHolder.getKieSession().fireAllRules();
        }
    }


    @Override
    public void visit(DeleteCommand command, boolean execute) {
        if(execute) {
            RemoteFactHandle remoteFH = command.getFactHandle();
            kieSessionHolder.getKieSession().getEntryPoint(command.getEntryPoint()).delete(fhMap.get(remoteFH));
            kieSessionHolder.getKieSession().fireAllRules();
        }
    }

    @Override
    public void visit(UpdateCommand command, boolean execute) {
        if(execute) {
            RemoteFactHandle remoteFH = command.getFactHandle();
            FactHandle factHandle = fhMap.get(remoteFH);
            kieSessionHolder.getKieSession().getEntryPoint(command.getEntryPoint()).update(factHandle, command.getObject());
            kieSessionHolder.getKieSession().fireAllRules();
        }
    }

    @Override
    public void visit(ListObjectsCommand command, boolean execute) {
        if(execute) {
            // @TODO StatefulKnowledgeSessionImpl$ObjectStoreWrapper isn't serializable going to create a list with serializable items
            Collection<? extends Object> objects = kieSessionHolder.getKieSession().getEntryPoint(command.getEntryPoint()).getObjects(command.getFilter());
            List serializableItems = new ArrayList<>(objects.size());
            Iterator<? extends Object> iterator = objects.iterator();
            while(iterator.hasNext()){
                Object o = iterator.next();
                serializableItems.add(o);
            }
            ListKieSessionObjectMessage msg = new ListKieSessionObjectMessage(command.getFactHandle().getId(), serializableItems);
            producer.produceSync(config.getKieSessionInfosTopicName(), command.getFactHandle().getId(), msg);
        }
    }

    @Override
    public void visit(FactCountCommand command, boolean execute) {
        if(execute) {
            FactCountMessage msg = new FactCountMessage(command.getFactHandle().getId(), kieSessionHolder.getKieSession().getFactCount());
            producer.produceSync(config.getKieSessionInfosTopicName(), command.getFactHandle().getId(), msg);
        }
    }

    @Override
    public void processWithSnapshot(ConsumerRecord record,
                                    State currentState,
                                    EventConsumer consumer,
                                    Queue<Object> sideEffects) {
        logger.info("SNAPSHOT !!!");
        // TODO add fhMap to snapshot image
        snapshooter.serialize(kieSessionHolder.getKieSession(), fhMap, record.key().toString(), record.offset());
        process(record, currentState, consumer, sideEffects);
    }

    public SessionSnapShooter getSnapshooter(){
        return snapshooter;
    }


}
