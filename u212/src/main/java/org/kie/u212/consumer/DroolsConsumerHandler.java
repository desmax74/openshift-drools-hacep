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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.time.SessionPseudoClock;
import org.kie.u212.Config;
import org.kie.u212.core.infra.election.State;
import org.kie.u212.core.infra.consumer.ConsumerHandler;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.producer.Producer;
import org.kie.u212.model.StockTickEvent;

public class DroolsConsumerHandler implements ConsumerHandler {

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
  public void process(ConsumerRecord record, State state) {
    if(state.equals(State.LEADER)){
      processAsAMaster(record);
    }else{
      processAsASlave(record);
    }
  }

  private void processAsAMaster(ConsumerRecord record){
    StockTickEvent stock = process(record);
    producer.produceFireAndForget(new ProducerRecord<>(Config.MASTER_TOPIC, stock.getId(), stock));
  }

  private StockTickEvent process(ConsumerRecord record) {
    StockTickEvent stock = (StockTickEvent) record.value();
    clock.advanceTime(stock.getTimestamp() - record.timestamp(), TimeUnit.MILLISECONDS);
    kieSession.insert(stock);
    return stock;
  }

  private void processAsASlave(ConsumerRecord record){
    StockTickEvent stock = process(record);
  }

}
