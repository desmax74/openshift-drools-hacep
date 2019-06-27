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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kie.u212.ConverterUtil;
import org.kie.u212.core.infra.election.State;

public class EventProducer<T> implements Producer, org.kie.u212.core.infra.election.Callback {

    protected org.apache.kafka.clients.producer.Producer<String, T> producer;

    private volatile boolean leader = false;

    @Override
    public void updateStatus(State state) {
        if (state.equals(State.LEADER) && !leader) {
            leader = true;
        } else if (state.equals(State.REPLICA) && leader) {
            leader = false;
        }
    }

    public void start(Properties properties) {
        producer = new KafkaProducer(properties);
    }

    public void stop() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    public void produceFireAndForget(String topicName, String key, Object object) {
        producer.send(getFreshProducerRecord(topicName, key, object));
    }


    public void produceSync(String topicName, String key, Object object) {
        producer.send(getFreshProducerRecord(topicName, key, object));
    }


    public void produceAsync(String topicName, String key, Object object, Callback callback) {
        producer.send(getFreshProducerRecord(topicName, key, object), callback);
    }


    private ProducerRecord<String, T> getFreshProducerRecord(String topicName, String key, Object object){
        return  new ProducerRecord<>(topicName, key, (T) ConverterUtil.serializeObj(object));
    }
}
