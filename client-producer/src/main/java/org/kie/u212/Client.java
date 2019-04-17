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
package org.kie.u212;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.utils.RecordMetadataUtil;
import org.kie.u212.model.EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private EventProducer producer;
    private String topic;

    public Client(String topic) {
        producer = new EventProducer<>();
        this.topic = topic;
    }

    public void start() {
        logger.info("Start client producer");
        producer.start(ClientUtils.getConfiguration(ClientUtils.PRODUCER_CONF));
    }

    @Override
    public void close() {
        logger.info("Closing client producer");
        producer.stop();
    }

    public RecordMetadata insertSync(EventWrapper event,
                                     boolean logInsert) {
        RecordMetadata lastRecord = producer.produceSync(new ProducerRecord<>(topic,
                                                                              event.getKey(),
                                                                              event));
        if (logInsert) {
            RecordMetadataUtil.logRecord(lastRecord);
        }
        return lastRecord;
    }

    public void insertAsync(EventWrapper event,
                            Callback callback) {
        producer.produceAsync(new ProducerRecord<>(topic,
                                                   event.getKey(),
                                                   event),
                              callback);
    }

    public Future<RecordMetadata> insertFireAndForget(EventWrapper event) {
        return producer.produceFireAndForget(new ProducerRecord<>(topic,
                                                                  event.getKey(),
                                                                  event));
    }


}
