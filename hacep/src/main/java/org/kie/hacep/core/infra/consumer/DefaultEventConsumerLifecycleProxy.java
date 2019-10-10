package org.kie.hacep.core.infra.consumer;

import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.DefaultSessionSnapShooter;

import org.kie.hacep.core.infra.election.State;

import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultEventConsumerLifecycleProxy<T> implements EventConsumerLifecycleProxy {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaConsumer.class);
    private Logger loggerForTest;
    private EventConsumerStatus status;
    private DroolsConsumerHandler consumerHandler;
    private EnvConfig config;
    private KafkaConsumers kafkaConsumers;

    public DefaultEventConsumerLifecycleProxy(DroolsConsumerHandler consumerHandler, EnvConfig config, DefaultSessionSnapShooter snapShooter){
        status = new EventConsumerStatus();
        this.consumerHandler = consumerHandler;
        this.config = config;
        if (this.config.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(this.config);
        }
        kafkaConsumers = new KafkaConsumers(status, config,this, this.consumerHandler, this.consumerHandler.getSnapshooter());
    }

    public KafkaConsumers getConsumers(){
        return  kafkaConsumers;
    }

    public EventConsumerStatus getStatus(){
        return status;
    }



    public void internalAskAndProcessSnapshotOnDemand() {
        status.setAskedSnapshotOnDemand(true);
        boolean completed = consumerHandler.initializeKieSessionFromSnapshotOnDemand(config);
        if (logger.isInfoEnabled()) {
            logger.info("askAndProcessSnapshotOnDemand:{}", completed);
        }
        if (!completed) {
            throw new RuntimeException("Can't obtain a snapshot on demand");
        }
    }


    public  void internalUpdateOnRunningConsumer(State state) {
        logger.info("updateOnRunning Consumer");
        if (state.equals(State.LEADER) ) {
            DroolsExecutor.setAsLeader();
            kafkaConsumers.internalRestart(state);
        } else if (state.equals(State.REPLICA)) {
            DroolsExecutor.setAsReplica();
            kafkaConsumers.internalRestart(state);
        }
    }

    @Override
    public void internalEnableConsumeAndStartLoop(State state) {
        kafkaConsumers.internalEnableConsumeAndStartLoop(state);
    }

    @Override
    public void internalStopConsume() {
        kafkaConsumers.stop();
    }
}
