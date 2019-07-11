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
package org.kie.remote.impl.consumer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.kie.remote.EnvConfig;
import org.kie.remote.impl.ClientUtils;

public class Listener {

    private Properties configuration;
    private EnvConfig envConfig;
    private Map<String, CompletableFuture<Object>> store;
    private Thread t;

    public Listener(Map<String, CompletableFuture<Object>> requestsStore){
        configuration = ClientUtils.getConfiguration(ClientUtils.CONSUMER_CONF);
        envConfig = EnvConfig.getDefaultEnvConfig();
        store = requestsStore;
    }

    public void listen(){
        t = new Thread(new ListenerThread(configuration, envConfig, store));
        t.start();
    }

    public void stopConsumeEvents(){
        if(t != null){
            try {
                t.join();
            }catch (InterruptedException ex){
                throw new RuntimeException(ex);
            }
        }
    }

}
