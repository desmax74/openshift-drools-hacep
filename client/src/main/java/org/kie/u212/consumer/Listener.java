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

import java.util.Map;
import java.util.Properties;

import org.kie.remote.RemoteFactHandle;
import org.kie.u212.ClientUtils;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.FactCountMessage;
import org.kie.u212.producer.RemoteKieSessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Listener {

    private static Logger logger = LoggerFactory.getLogger(RemoteKieSessionImpl.class);
    private Properties configuration;
    private EnvConfig envConfig;
    private Map<String, Object> store;

    public Listener(Map<String, Object> requestsStore){
        configuration = ClientUtils.getConfiguration(ClientUtils.CONSUMER_CONF);
        envConfig = EnvConfig.getDefaultEnvConfig();
        store = requestsStore;
    }

    public void retrieveAndStoreFactCount(RemoteFactHandle factHandle){
        FactCountMessage msg  = ConsumerUtils.getFactCount(factHandle, envConfig, configuration);
        store.put(factHandle.getId(), msg.getFactCount());
    }

    public void retrieveAndStoreObjects(RemoteFactHandle factHandle){
        Object value  = ConsumerUtils.getObjects(factHandle, envConfig, configuration);
        store.put(factHandle.getId(), value);
    }

    public void retrieveAndStoreObjectsFiltered(RemoteFactHandle factHandle){
        Object value  = ConsumerUtils.getObjectsFiltered(factHandle, envConfig, configuration);
        store.put(factHandle.getId(), value);
    }

}
