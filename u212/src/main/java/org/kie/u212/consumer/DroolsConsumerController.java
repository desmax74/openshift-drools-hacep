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

import org.kie.u212.Config;
import org.kie.u212.infra.consumer.ConsumerThread;
import org.kie.u212.model.StockTickEvent;

public class DroolsConsumerController {

    public DroolsConsumerController(){}

    public void consumeEvents(int numberOfConsumer, String groupName, int duration, int pollSize) {
        for(int i = 0; i < numberOfConsumer; i++) {
            Thread t = new Thread(
                    new ConsumerThread<StockTickEvent>(
                            String.valueOf(i),
                            groupName,
                            Config.MASTER_TOPIC,
                            "org.kie.u212.consumer.EventJsonSerializer",
                            pollSize,
                            duration,
                            false ,
                            true,
                            true,
                            new DroolsConsumerHandler()));
            t.start();
        }
    }

    public void consumeEvents(String groupName, int duration, int pollSize) {
        Thread t = new Thread(
                new ConsumerThread<StockTickEvent>(
                        "1",
                        groupName,
                        Config.MASTER_TOPIC,
                        "org.kie.u212.consumer.EventJsonSerializer",
                        pollSize,
                        duration,
                        false ,
                        true,
                        true,
                        new DroolsConsumerHandler()));
        t.start();
    }

}
