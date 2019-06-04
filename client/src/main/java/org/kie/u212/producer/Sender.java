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
package org.kie.u212.producer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.u212.ClientUtils;
import org.kie.u212.Config;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.utils.RecordMetadataUtil;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sender {

  private static Logger logger = LoggerFactory.getLogger(ClientProducer.class);
  private EventProducer producer;
  private Properties configuration;
  private EnvConfig envConfig;

  public Sender(Properties configuration, EnvConfig envConfig){
    this.envConfig = envConfig;
    producer = new EventProducer();
    if(configuration != null && !configuration.isEmpty()) {
      this.configuration = configuration;
    }
  }

  public void start() {
    logger.info("Start client producer");
    producer.start(configuration != null ? configuration : ClientUtils.getConfiguration(ClientUtils.PRODUCER_CONF));
  }

  public void stop() {
    logger.info("Closing client producer");
    producer.stop();
  }

  public String insertSync(Object obj, boolean logInsert) {
    EventWrapper event = wrapObject(obj);
    RecordMetadata lastRecord = producer.produceSync(new ProducerRecord<>(envConfig.getEventsTopicName(), event.getKey(), event));
    if (logInsert) {
      RecordMetadataUtil.logRecord(lastRecord);
    }
    return lastRecord.toString();
  }

  public void insertAsync(Object obj,
                          Callback callback) {
    EventWrapper event = wrapObject(obj);
    producer.produceAsync(new ProducerRecord<>(envConfig.getEventsTopicName(),
                                               event.getKey(),
                                               event),
                          callback);
  }

  public Future<RecordMetadata> insertFireAndForget(Object obj) {
    EventWrapper event = (EventWrapper) obj;
    return producer.produceFireAndForget(new ProducerRecord<>(envConfig.getEventsTopicName(),
                                                              event.getKey(),
                                                              event));
  }

  private EventWrapper wrapObject(Object obj){
    EventWrapper event = new EventWrapper(obj,
                                          UUID.randomUUID().toString(),
                                          0l,
                                          EventType.APP);
    return  event;
  }
}
