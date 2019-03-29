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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;

public class ClientProducerTest {

    public static void main(String[] args) throws Exception {
        insertBatchEvent(2);
    }

    private static void insertBatchEvent(int items) {
        Client client = new Client(Config.CONTROL_TOPIC);
        client.start();
        for (int i = 0; i < items; i++) {
            StockTickEvent eventA = new StockTickEvent("RHT",
                                                       ThreadLocalRandom.current().nextLong(80,
                                                                                            100));

            EventWrapper wr = new EventWrapper(eventA,
                                               UUID.randomUUID().toString(),
                                               0l,
                                               EventType.APP);
            client.insertSync(wr,
                              true);
        }
        client.close();
    }
}
