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
package org.kie.hacep.core;

import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.DefaultSessionSnapShooter;
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.DefaultKafkaConsumer;
import org.kie.hacep.core.infra.consumer.EventConsumer;
import org.kie.hacep.core.infra.consumer.LocalConsumer;
import org.kie.remote.impl.producer.Producer;

public class InfraFactory {

    public static EventConsumer getEventConsumer(EnvConfig config) {
        return config.isLocal() ? new LocalConsumer(config) : new DefaultKafkaConsumer(config);
    }

    public static SessionSnapshooter getSnapshooter(EnvConfig envConfig) {
        return new DefaultSessionSnapShooter(envConfig);
    }

    public static ConsumerHandler getConsumerHandler(Producer producer, EnvConfig envConfig) {
        return (ConsumerHandler) new DroolsConsumerHandler(producer, envConfig, getSnapshooter(envConfig));
    }
}
