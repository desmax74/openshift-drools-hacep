/*
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
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
package org.kie.remote;

import java.util.Properties;

import org.kie.remote.impl.ClientUtils;

import org.kie.remote.impl.RemoteStreamingKieSessionImpl;
import org.kie.remote.impl.consumer.KafkaListenerThread;
import org.kie.remote.impl.consumer.Listener;
import org.kie.remote.impl.consumer.ListenerThread;
import org.kie.remote.impl.consumer.LocalListenerThread;
import org.kie.remote.impl.producer.EventProducer;
import org.kie.remote.impl.producer.LocalProducer;
import org.kie.remote.impl.producer.Producer;

/**
 * Factory to build Kafka related classes in a single place
 * */
public class InfraFactory {

    private InfraFactory(){}

    /**
     * Build a Listener, local for test or on Kafka
     * */
    public static Listener getListener(Properties props, boolean isLocal){
        return new Listener(props, InfraFactory.getListenerThread(TopicsConfig.getDefaultTopicsConfig(), isLocal, props));
    }

    /***
     * Build a ListenerThread, local for test or on Kafka
     */
    public static ListenerThread getListenerThread(TopicsConfig topicsConfig, boolean isLocal, Properties configuration) {
        return isLocal ?
                new LocalListenerThread(topicsConfig) :
                new KafkaListenerThread(getMergedConf(configuration), topicsConfig);
    }

    private static Properties getMergedConf(Properties configuration) {
        Properties conf = ClientUtils.getConfiguration(ClientUtils.CONSUMER_CONF);
        conf.putAll(configuration);
        return conf;
    }

    /**
     * Build a event Producer, local for test or on Kafka
     * */
    public static Producer getProducer(boolean isLocal) {
        return isLocal ? new LocalProducer() : new EventProducer();
    }

    /**
     * Build a RemoteStreamingKieSession
     * */
    public static RemoteStreamingKieSession createRemoteStreamingKieSession(Properties configuration, TopicsConfig envConfig, Listener listener, Producer producer) {
        return new RemoteStreamingKieSessionImpl(configuration, envConfig, listener, producer);
    }

}
