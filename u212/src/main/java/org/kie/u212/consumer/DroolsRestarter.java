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

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kie.u212.core.Config;
import org.kie.u212.election.Callback;
import org.kie.u212.infra.PartitionListener;

/***
 * Purpose of this class is to set a new consumer
 * when a changeTopic in the DroolsConsumer is called without leave
 * the ConsumerThread's inner loop
 */
public class DroolsRestarter {

  private DroolsConsumer consumer;
  private DroolsCallback callback;

  public DroolsRestarter(){
    callback = new DroolsCallback();
  }

  public void createDroolsConsumer(String id){
    consumer = new DroolsConsumer(id,this);
    callback.setConsumer(consumer);
  }

  public void changeTopic(String newTopic, Map<TopicPartition, OffsetAndMetadata> offsets){
    Consumer kafkaConsumer =  consumer.getKafkaConsumer();
    Consumer newConsumer = new KafkaConsumer<>(Config.getDefaultConfig());
    newConsumer.subscribe(Collections.singletonList(newTopic), new PartitionListener(newConsumer, offsets));
    consumer.setKafkaConsumer(newConsumer);
    consumer.internalStart();
    kafkaConsumer.close();
    kafkaConsumer = null;
  }

  public DroolsConsumer getConsumer() {
    return consumer;
  }

  public void setConsumer(DroolsConsumer consumer) {
    this.consumer = consumer;
  }

  public Callback getCallback() {
    return callback;
  }

}
