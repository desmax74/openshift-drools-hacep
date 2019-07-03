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
package org.kie.hacep.core.infra.consumer;

import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.KieSessionHolder;
import org.kie.hacep.core.infra.election.LeadershipCallback;
import org.kie.hacep.core.infra.utils.Printer;

/***
 * Purpose of this class is to set a new consumer
 * when a changeTopic in the DroolsConsumer is called without leave
 * the ConsumerThread's inner loop
 */
public class Restarter {

    private DefaultConsumer consumer;
    private InfraCallback callback;
    private Printer printer;
    private KieSessionHolder kieSessionHolder;

    public Restarter(Printer printer, KieSessionHolder kieSessionHolder) {
        callback = new InfraCallback();
        this.printer = printer;
        this.kieSessionHolder = kieSessionHolder;
    }

    public void createDroolsConsumer(EnvConfig envConfig) {
        consumer = new DefaultConsumer(this, printer, envConfig);
        callback.setConsumer(consumer);
    }


    public DefaultConsumer getConsumer() {
        return consumer;
    }

    public KieSessionHolder getKieSessionHolder() {
        return kieSessionHolder;
    }

    public void setConsumer(DefaultConsumer consumer) {
        this.consumer = consumer;
    }

    public LeadershipCallback getCallback() {
        return callback;
    }
}
