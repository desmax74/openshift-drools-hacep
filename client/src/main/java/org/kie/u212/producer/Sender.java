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
import org.kie.remote.RemoteCommand;
import org.kie.u212.ClientUtils;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.model.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.u212.core.infra.utils.RecordMetadataUtil.logRecord;

public class Sender {

  private static Logger logger = LoggerFactory.getLogger( RemoteKieSessionImpl.class);
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

  public RecordMetadata sendCommand( RemoteCommand command ) {
    RecordMetadata lastRecord = producer.produceSync(new ProducerRecord<>(envConfig.getEventsTopicName(), command.getId(), command));
    return logRecord(lastRecord);
  }

  // TODO is this useful? I think we should remove it
  public void insertAsync(Object obj,
                          Callback callback) {
    ControlMessage event = wrapObject(obj);
    producer.produceAsync(new ProducerRecord<>(envConfig.getEventsTopicName(),
                                               event.getKey(),
                                               event),
                          callback);
  }

  public Future<RecordMetadata> insertFireAndForget(Object obj) {
    ControlMessage event = ( ControlMessage ) obj;
    return producer.produceFireAndForget(new ProducerRecord<>(envConfig.getEventsTopicName(),
                                                              event.getKey(),
                                                              event));
  }

  private ControlMessage wrapObject( Object obj){
    ControlMessage event = new ControlMessage(UUID.randomUUID().toString(), 0l);
    return  event;
  }
}
