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

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.time.SessionPseudoClock;
import org.kie.u212.model.StockTickEvent;
import org.kie.u212.infra.consumer.ConsumerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsConsumerHandler implements ConsumerHandler {

    private Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private KieContainer kieContainer;
    private KieSession kieSession;
    private SessionPseudoClock clock;

    public DroolsConsumerHandler() {
        kieContainer = KieServices.get().newKieClasspathContainer();
        kieSession = kieContainer.newKieSession();
        clock = kieSession.getSessionClock();
    }

    @Override
    public void process(ConsumerRecord record) {
        StockTickEvent stock = (StockTickEvent) record.value();
        clock.advanceTime(stock.getTimestamp() - clock.getCurrentTime(), TimeUnit.MILLISECONDS);
        kieSession.insert(stock);
        logger.info("Process stock:{}", record);
    }
}
