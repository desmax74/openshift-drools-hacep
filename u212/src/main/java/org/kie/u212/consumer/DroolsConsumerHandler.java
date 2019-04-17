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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.time.SessionPseudoClock;
import org.kie.u212.Config;
import org.kie.u212.ConverterUtil;
import org.kie.u212.core.infra.consumer.ConsumerHandler;
import org.kie.u212.core.infra.consumer.EventConsumer;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.producer.Producer;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.kie.u212.producer.SessionSnaptshooter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsConsumerHandler implements ConsumerHandler {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private KieContainer kieContainer;
    private KieSession kieSession;
    private SessionPseudoClock clock;
    private Producer producer;
    private SessionSnaptshooter snapshooter;
    private Properties configuration;

    public DroolsConsumerHandler(EventProducer producer, Properties configuration) {
        kieContainer = KieServices.get().newKieClasspathContainer();
        kieSession = kieContainer.newKieSession();
        clock = kieSession.getSessionClock();
        this.producer = producer;
        this.configuration = configuration;
        snapshooter = new SessionSnaptshooter(configuration);
    }

    @Override
    public void process(ConsumerRecord record,
                        State state,
                        EventConsumer consumer) {
        if (state.equals(State.LEADER)) {
            processAsMaster(record);
        } else {
            processAsASlave(record);
        }
    }

    @Override
    public void processWithSnapshot(ConsumerRecord record,
                                    State currentState,
                                    EventConsumer consumer) {
        logger.info("SNAPSHOT !!!");
        snapshooter.serialize(kieSession);
        process(record, currentState, consumer);


    }

    private void processAsMaster(ConsumerRecord record) {
        EventWrapper wr = (EventWrapper) record.value();
        switch (wr.getEventType()) {
            case APP:
                StockTickEvent stock = process(record);
                EventWrapper newEventWrapper = new EventWrapper(stock, wr.getKey(), 0l, EventType.APP);
                producer.produceSync(new ProducerRecord<>(Config.CONTROL_TOPIC, wr.getKey(), newEventWrapper));
                break;
            default:
                logger.info("Event type not handled:{}", wr.getEventType());
        }
    }

    private StockTickEvent process(ConsumerRecord record) {
        EventWrapper wr = (EventWrapper) record.value();
        Map map = (Map) wr.getDomainEvent();
        StockTickEvent stockTickEvent = ConverterUtil.fromMap(map);
        stockTickEvent.setTimestamp(record.timestamp());
        clock.advanceTime(stockTickEvent.getTimestamp() - record.timestamp(), TimeUnit.MILLISECONDS);
        kieSession.insert(stockTickEvent);
        return stockTickEvent;
    }

    private long processAsASlave(ConsumerRecord record) {
        StockTickEvent stock = process(record);
        return 0l;
    }
}
