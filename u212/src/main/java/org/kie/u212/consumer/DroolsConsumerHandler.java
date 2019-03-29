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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.time.SessionPseudoClock;
import org.kie.u212.Config;
import org.kie.u212.core.infra.consumer.ConsumerHandler;
import org.kie.u212.core.infra.consumer.EventConsumer;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.producer.Producer;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsConsumerHandler implements ConsumerHandler {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private KieContainer kieContainer;
    private KieSession kieSession;
    private SessionPseudoClock clock;
    private Producer producer;

    public DroolsConsumerHandler(EventProducer producer) {
        kieContainer = KieServices.get().newKieClasspathContainer();
        kieSession = kieContainer.newKieSession();
        clock = kieSession.getSessionClock();
        this.producer = producer;
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

    private void processAsMaster(ConsumerRecord record) {
        EventWrapper wr = (EventWrapper) record.value();
        switch (wr.getEventType()) {
            case APP:
                StockTickEvent stock = process(record);
                EventWrapper newEventWrapper = new EventWrapper(stock, wr.getID(), 0l, EventType.APP);
                producer.produceFireAndForget(new ProducerRecord<>(Config.CONTROL_TOPIC,
                                                                   wr.getID(),
                                                                   newEventWrapper));
                break;
            default:
                logger.info("Event type not handled:{}", wr.getEventType());
        }
    }

    private StockTickEvent process(ConsumerRecord record) {
        EventWrapper wr = (EventWrapper) record.value();
        Map map = (Map) wr.getDomainEvent();
        StockTickEvent stockTickEvent = new StockTickEvent();
        stockTickEvent.setCompany(map.get("company").toString());
        stockTickEvent.setPrice((Double) map.get("price"));
        stockTickEvent.setTimestamp(record.timestamp());
        clock.advanceTime(stockTickEvent.getTimestamp() - record.timestamp(), TimeUnit.MILLISECONDS);
        kieSession.insert(stockTickEvent);
        return stockTickEvent;
    }

    private void processAsASlave(ConsumerRecord record) {
        StockTickEvent stock = process(record);
    }
}
