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
package org.kie.u212.core.infra.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.u212.core.infra.election.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProducer<T> extends AbstractProducer<String, T> implements Producer<String, T>, org.kie.u212.core.infra.election.Callback {

  private Logger logger = LoggerFactory.getLogger(EventProducer.class);

  private volatile boolean leader = false;

  public void start(Properties properties) {
    producer = new KafkaProducer(properties);
  }

  public void start(KafkaProducer<String, T> kafkaProducer) {
    producer = kafkaProducer;
  }

  public void stop() {
    if(producer != null) {
      producer.close();
    }
  }

  public Future<RecordMetadata> produceFireAndForget(ProducerRecord<String, T> producerRecord) {
    return producer.send(producerRecord);
  }

  public RecordMetadata produceSync(ProducerRecord<String, T> producerRecord) {
    RecordMetadata recordMetadata = null;
    try {
      recordMetadata = producer.send(producerRecord).get();
    } catch (InterruptedException e) {
      logger.error("Error in produceSync!", e);
    } catch (ExecutionException e) {
      logger.error("Error in produceSync!", e);
    }
    return recordMetadata;
  }

  @Override
  public void produceAsync(ProducerRecord<String, T> producerRecord, Callback callback) {
    producer.send(producerRecord, callback);
  }

  @Override
  public void updateStatus(State state) {
    if(state.equals(State.LEADER) && !leader){
      leader = true;
    }else if(state.equals(State.NOT_LEADER) && leader){
      leader = false;
    }
  }
}
