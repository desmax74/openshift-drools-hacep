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

import org.kie.u212.infra.consumer.ConsumerThread;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//@TODO Only for manual Demo
public class DroolsConsumerController<T> {

  private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerController.class);
  private DroolsConsumer<StockTickEvent> consumer;

  public DroolsConsumerController(DroolsConsumer<StockTickEvent> consumer) {
    this.consumer = consumer;
  }

  public DroolsConsumer getConsumer(){
    return consumer;
  }


  public void consumeEvents(String topic, String groupName, int duration, int pollSize) {
    logger.info("Starting CONSUMING event on topic :{}", topic);
    Thread t = new Thread(
            new ConsumerThread<>(
                    "1",
                    groupName,
                    topic,
                    "org.kie.u212.consumer.EventJsonSerializer",
                    pollSize,
                    duration,
                    false,
                    true,
                    true,
                    consumer));
    t.start();
  }
}
