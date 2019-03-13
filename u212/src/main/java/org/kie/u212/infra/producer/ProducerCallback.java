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
package org.kie.u212.infra.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {

  private Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

  @Override
  public void onCompletion(RecordMetadata recordMetadata,
                           Exception e) {
    if (e != null) {
      logger.error(e.getMessage(),
                   e);
    } else {
      logger.info("AsynchronousProducer call success ! \n Topic:{} \n Partition:{} \n Offset:{} \n Timestamp:{} \n",
                  recordMetadata.topic(),
                  recordMetadata.partition(),
                  recordMetadata.offset(),
                  recordMetadata.timestamp());
    }
  }
}
