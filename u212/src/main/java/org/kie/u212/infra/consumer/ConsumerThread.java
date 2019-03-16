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

import org.kie.u212.consumer.DroolsConsumer;

public class ConsumerThread implements Runnable {

    private int size;
    private long duration;
    private boolean commitSync;
    private boolean subscribeMode;
    private DroolsConsumer consumer;

    public ConsumerThread(
            int pollSize,
            long duration,
            boolean commitSync,
            boolean subscribeMode,
            DroolsConsumer consumer) {
        this.size = pollSize;
        this.duration = duration;
        this.commitSync = commitSync;
        this.subscribeMode = subscribeMode;
        this.consumer = consumer;
    }

    public void run() {
        consumer.setSubscribeMode(subscribeMode);
        consumer.poll(size, duration, commitSync); //delayed to the first status update
    }
}
