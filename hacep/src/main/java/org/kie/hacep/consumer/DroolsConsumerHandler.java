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
import org.kie.api.builder.KieScanner;
import org.kie.api.runtime.KieContainer;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.GlobalStatus;
import org.kie.hacep.core.KieSessionContext;
import org.kie.hacep.core.infra.DefaultSessionSnapShooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.ItemToProcess;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.SnapshotOnDemandUtils;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.kie.remote.command.RemoteCommand;
import org.kie.remote.command.VisitableCommand;
import org.kie.remote.impl.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;

public class DroolsConsumerHandler implements ConsumerHandler {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private Logger loggerForTest;
    private Producer producer;
    private DefaultSessionSnapShooter sessionSnapShooter;
    private EnvConfig envConfig;
    private KieSessionContext kieSessionContext;
    private CommandHandler commandHandler;
    private SnapshotInfos snapshotInfos;
    private boolean shutdown;

    public DroolsConsumerHandler(Producer producer, EnvConfig envConfig) {
        this.envConfig = envConfig;
        this.producer = producer;
        this.sessionSnapShooter = new DefaultSessionSnapShooter(this.envConfig);
        initializeKieSessionContextFromSnapshot(this.envConfig);
        commandHandler = new CommandHandler(kieSessionContext, this.envConfig, producer, sessionSnapShooter);
        if (this.envConfig.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(envConfig);
        }
    }

    private void initializeKieSessionContextFromSnapshot(EnvConfig config) {
        if(config.isSkipOnDemanSnapshot()) {// if true we reads the snapshots and wait until the first leaderElectionUpdate
            this.snapshotInfos = sessionSnapShooter.deserialize();
            /* if the Snapshot is ok we use the KieContainer and KieSession from the infos,
            otherwise (empty system) we create kiecontainer and kieSession from the envConfig's GAV */
            if (snapshotInfos != null) {
                kieSessionContext = initializeKieSessionContext(snapshotInfos);
            }else{
                KieContainer kieContainer;
                if(envConfig.isUpdatableKJar()) {
                    String[] parts = envConfig.getKjarGAV().split(":");
                    kieContainer = KieServices.get().newKieContainer(KieServices.get().newReleaseId(parts[0], parts[1], parts[2]));
                }else{
                    kieContainer = KieServices.get().newKieClasspathContainer();
                }
                kieSessionContext = new KieSessionContext();
                kieSessionContext.init(kieContainer, kieContainer.newKieSession());
            }

        } else{
            kieSessionContext = new KieSessionContext();
            createClasspathSession( kieSessionContext );
        }
    }

    public boolean initializeKieSessionFromSnapshotOnDemand(EnvConfig config) {
        if(!config.isSkipOnDemanSnapshot()) {// if true we reads the snapshots and wait until the first leaderElectionUpdate
            this.snapshotInfos = SnapshotOnDemandUtils.askASnapshotOnDemand(config,
                                                                            sessionSnapShooter);
            kieSessionContext = initializeKieSessionContext(snapshotInfos);
            this.sessionSnapShooter = new DefaultSessionSnapShooter(this.envConfig);
            return true;
        }
        return false;
    }

    public DefaultSessionSnapShooter getSessionSnapShooter(){
        return sessionSnapShooter;
    }

    @Override
    public void process( ItemToProcess item, State state) {
        RemoteCommand command  = deserialize((byte[])item.getObject());
        process( command, state );
    }

    @Override
    public void process( RemoteCommand command, State state ) {
        if(envConfig.isUnderTest()) {  loggerForTest.warn("DroolsConsumerHandler.process Remote command on process:{} state:{}", command, state); }
        if (state.equals(State.LEADER)) {
            processCommand( command, state );
            Queue<Object> sideEffectsResults = DroolsExecutor.getInstance().getAndReset();
            if (envConfig.isUnderTest()) { loggerForTest.warn("DroolsConsumerHandler.process sideEffects:{}", sideEffectsResults); }
            ControlMessage newControlMessage = new ControlMessage(command.getId(), sideEffectsResults);
            if (envConfig.isUnderTest()) { loggerForTest.warn("DroolsConsumerHandler.process new ControlMessage sent to control topic:{}", newControlMessage); }
            producer.produceSync(envConfig.getControlTopicName(), command.getId(), newControlMessage);
            if (envConfig.isUnderTest()) { loggerForTest.warn("sideEffectOnLeader:{}", sideEffectsResults); }
        } else {
            processCommand( command, state );
        }
    }

    public void processSideEffectsOnReplica(Queue<Object> newSideEffects) {
        DroolsExecutor.getInstance().appendSideEffects(newSideEffects);
        if(envConfig.isUnderTest()){ loggerForTest.warn("sideEffectOnReplica:{}", newSideEffects);}
    }

    @Override
    public void processWithSnapshot(ItemToProcess item, State currentState) {
        if (logger.isInfoEnabled()){ logger.info("SNAPSHOT"); }
        process(item, currentState);
        if(!shutdown) {
            sessionSnapShooter.serialize(kieSessionContext, item.getKey(), item.getOffset());
        }
    }

    @Override
    public void stop() {
        shutdown = true;
        if(kieSessionContext != null) {
            kieSessionContext.getKieSession().dispose();
        }
    }

    private void processCommand( RemoteCommand command, State state ) {
        boolean execute = state.equals(State.LEADER) || command.isPermittedForReplicas();
        if (execute) {
            VisitableCommand visitable = (VisitableCommand) command;
            try {
                visitable.accept(commandHandler);
            } catch (Throwable e) {
                GlobalStatus.nodeLive = false;
                throw e;
            }
        }
    }

    public KieSessionContext initializeKieSessionContext(SnapshotInfos infos){
        KieSessionContext kieSessionContext = getKieSessionContext();
        if (infos != null) {
            if (logger.isInfoEnabled()) { logger.info("start consumer with:{}", infos); }
            initSessionHolder(infos, kieSessionContext);
        } else {
            createClasspathSession(kieSessionContext);
        }
        return kieSessionContext;
    }

    private KieSessionContext getKieSessionContext() {
        KieSessionContext kieSessionContext;
        if(envConfig.isUpdatableKJar()){
            kieSessionContext = deployKJar(envConfig.getKjarGAV());
        }else {
            kieSessionContext = new KieSessionContext();
        }
        return kieSessionContext;
    }

    private void createClasspathSession( KieSessionContext kieSessionContext ) {
        KieServices srv = KieServices.get();
        if (srv != null) {
            if(envConfig.isUpdatableKJar()){
                if (logger.isInfoEnabled()) {logger.info("Creating new KieContainer with KJar:{}", envConfig.getKjarGAV());}
                String parts[] = envConfig.getKjarGAV().split(":");
                KieContainer kieContainer = srv.newKieContainer(srv.newReleaseId(parts[0], parts[1], parts[2]));
                KieScanner scanner = srv.newKieScanner(kieContainer);
                scanner.scanNow();
                kieSessionContext.init(kieContainer, kieContainer.newKieSession());
            }else {
                if (logger.isInfoEnabled()) {
                    logger.info("Creating new Kie Session");
                }
                KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
                kieSessionContext.init(kieContainer, kieContainer.newKieSession());
            }
        } else {
            logger.error("KieService is null");
        }
    }

    private void initSessionHolder(SnapshotInfos infos, KieSessionContext kieSessionContext) {
        if (infos.getKieSession() == null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            kieSessionContext.init(kieContainer, kieContainer.newKieSession());
        } else {
            if(logger.isInfoEnabled()){ logger.info("Applying snapshot");}
            kieSessionContext.initFromSnapshot(infos);
        }
    }

    public KieSessionContext deployKJar(String gav){
        String parts[]= gav.split(":");
        return deployKJar(parts[0], parts[1], parts[2]);
    }

    public KieSessionContext deployKJar(String groupId, String artifactId, String version) {
        KieSessionContext kieSessionContext = new KieSessionContext();
        KieServices ks = KieServices.get();
        if (ks != null) {
            KieContainer kieContainer = ks.newKieContainer(ks.newReleaseId(groupId, artifactId, version));
            KieScanner scanner = ks.newKieScanner(kieContainer);
            scanner.scanNow();
            kieSessionContext.init(kieContainer, kieContainer.newKieSession());
        } else {
            logger.error("KieService is null");
        }
        return kieSessionContext;
    }
}