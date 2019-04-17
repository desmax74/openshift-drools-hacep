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
package org.kie.u212.core.infra.consumer;

import org.kie.u212.core.infra.election.Callback;

/***
 * Purpose of this class is to set a new consumer
 * when a changeTopic in the DroolsConsumer is called without leave
 * the ConsumerThread's inner loop
 */
public class Restarter {

    private DefaultConsumer consumer;
    private InfraCallback callback;

    public Restarter() {
        callback = new InfraCallback();
    }

    public void createDroolsConsumer() {
        consumer = new DefaultConsumer(this);
        callback.setConsumer(consumer);
    }


    public DefaultConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(DefaultConsumer consumer) {
        this.consumer = consumer;
    }

    public Callback getCallback() {
        return callback;
    }
}
