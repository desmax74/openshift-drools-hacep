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
package org.kie.u212.infra.consumer;

import java.util.Properties;

import org.kie.u212.consumer.DroolsConsumer;

public class ConsumerThread<T> implements Runnable {

  private String id;
  private String groupId;
  private String topic;
  private String deserializerClass;
  private int size;
  private long duration;
  private boolean autoCommit;
  private boolean commitSync;
  private boolean subscribeMode;
  private ConsumerHandler consumerHandle;

  public ConsumerThread(
          String id,
          String groupId,
          String topic,
          String deserializerClass,
          int pollSize,
          long duration,
          boolean autoCommit,
          boolean commitSync,
          boolean subscribeMode,
          ConsumerHandler consumerHandle) {
    this.id = id;
    this.groupId = groupId;
    this.topic = topic;
    this.deserializerClass = deserializerClass;
    this.size = pollSize;
    this.duration = duration;
    this.autoCommit = autoCommit;
    this.commitSync = commitSync;
    this.subscribeMode = subscribeMode;
    this.consumerHandle = consumerHandle;
  }

  public void run() {
    Properties properties = new Properties();
    properties.setProperty("key.deserializer",
                           deserializerClass);
    DroolsConsumer<T> consumer = new DroolsConsumer<>(id,
                                                      properties,
                                                      consumerHandle);
    if (subscribeMode) {
      consumer.subscribe(groupId,
                         topic,
                         autoCommit);
    } else {
      consumer.assign(topic,
                      null,
                      autoCommit);
    }
    consumer.poll(size,
                  duration,
                  commitSync);
  }
}
