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

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.time.SessionPseudoClock;
import org.kie.hacep.ConverterUtil;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.KieSessionContext;
import org.kie.hacep.core.infra.DeafultSessionSnapShooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
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
    private DeafultSessionSnapShooter snapshooter;
    private EnvConfig config;
    private KieSessionContext kieSessionContext;
    private CommandHandler commandHandler;
    private SnapshotInfos infos;

    public DroolsConsumerHandler(EventProducer producer, EnvConfig envConfig) {
        this.snapshooter = new DeafultSessionSnapShooter(envConfig);
        this.infos = snapshooter.deserialize();
        this.kieSessionContext = createSessionHolder( infos );
        clock = kieSessionContext.getKieSession().getSessionClock();
        this.config = envConfig;
        this.producer = producer;
        commandHandler = new CommandHandler(kieSessionContext, config, producer);
    }

    public DeafultSessionSnapShooter getSnapshooter(){
        return snapshooter;
    }

    public void process( ItemToProcess item, State state, Queue<Object> sideEffects) {
        RemoteCommand command  = ConverterUtil.deSerializeObjInto((byte[])item.getObject(), RemoteCommand.class);
        processCommand( command, state );

        if (state.equals(State.LEADER)) {
            Queue<Object> results = DroolsExecutor.getInstance().getAndReset();
            ControlMessage newControlMessage = new ControlMessage(command.getId(), results);
            producer.produceSync(config.getControlTopicName(), command.getId(), newControlMessage);
        }else{
            if(sideEffects != null) {
                DroolsExecutor.getInstance().setResult(sideEffects);
            }
        }
    }


    public void processWithSnapshot(ItemToProcess item,
                                    State currentState, Queue<Object> sideEffects) {
        logger.info("SNAPSHOT !!!");
        snapshooter.serialize(kieSessionContext, item.getKey(), item.getOffset());
        process(item, currentState, sideEffects);
    }

    @Override
    public void stop() {
        kieSessionContext.getKieSession().dispose();
        snapshooter.stop();
    }

    private void processCommand( RemoteCommand command, State state ) {
        boolean execute = state.equals(State.LEADER) || command.isPermittedForReplicas();
        VisitableCommand visitable = (VisitableCommand) command;
        if (execute) {
            visitable.accept(commandHandler);
        }
    }

    private KieSessionContext createSessionHolder(SnapshotInfos infos ) {
        KieSessionContext kieSessionContext = new KieSessionContext();
        if (infos != null) {
            logger.info("start consumer with:{}", infos);
            initSessionHolder( infos, kieSessionContext );
        } else {
            createClasspathSession( kieSessionContext );
        }
        return kieSessionContext;
    }

    private void createClasspathSession( KieSessionContext kieSessionContext ) {
        KieServices srv = KieServices.get();
        if (srv != null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            logger.info("Creating new Kie Session");
            kieSessionContext.init(kieContainer.newKieSession());
        } else {
            logger.error("KieService is null");
        }
    }

    private void initSessionHolder(SnapshotInfos infos, KieSessionContext kieSessionContext) {
        if (infos.getKieSession() == null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            kieSessionContext.init(kieContainer.newKieSession());
        } else {
            logger.info("Applying snapshot");
            kieSessionContext.initFromSnapshot(infos);
        }
    }
}