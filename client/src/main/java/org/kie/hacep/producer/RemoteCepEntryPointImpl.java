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
package org.kie.hacep.producer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.kie.remote.RemoteCepEntryPoint;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.ListObjectsCommandClassType;
import org.kie.remote.command.ListObjectsCommandNamedQuery;
import org.kie.remote.command.ListObjectsCommand;
import org.kie.remote.impl.RemoteFactHandleImpl;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.Listener;

public class RemoteCepEntryPointImpl implements RemoteCepEntryPoint {

    protected final Sender sender;
    protected final Listener listener;
    private final String entryPoint;
    private Map<String, Object> requestsStore;
    private EnvConfig envConfig;

    public RemoteCepEntryPointImpl(Sender sender, String entryPoint, EnvConfig envConfig ) {
        this.sender = sender;
        this.entryPoint = entryPoint;
        this.envConfig = envConfig;
        requestsStore = new ConcurrentHashMap<>();
        listener = new Listener(requestsStore);
    }

    public void listen(){
        listener.listen();
    }

    @Override
    public String getEntryPointId() {
        return entryPoint;
    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects() {
        CompletableFuture callback = new CompletableFuture<>();
        ListObjectsCommand command = new ListObjectsCommand(entryPoint);
        requestsStore.put(command.getId(), callback);
        sender.sendCommand(command, envConfig.getEventsTopicName());
        return callback;
    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects(Class clazztype) {
        CompletableFuture callback = new CompletableFuture<>();
        ListObjectsCommand command = new ListObjectsCommandClassType(entryPoint, clazztype);
        requestsStore.put(command.getId(), callback);
        sender.sendCommand(command, envConfig.getEventsTopicName());
        return callback;
    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects(String namedQuery, String objectName, Object... params) {
        CompletableFuture callback = new CompletableFuture<>();
        ListObjectsCommand command = new ListObjectsCommandNamedQuery(entryPoint, namedQuery, objectName, params);
        requestsStore.put(command.getId(), callback);
        sender.sendCommand(command, envConfig.getEventsTopicName());
        return callback;
    }

    @Override
    public CompletableFuture<Long> getFactCount() {
        CompletableFuture callback = new CompletableFuture<>();
        FactCountCommand command = new FactCountCommand(entryPoint );
        requestsStore.put(command.getId(), callback);
        sender.sendCommand(command, envConfig.getEventsTopicName());
        return callback;
    }

    @Override
    public void insert(Object object) {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl(object );
        InsertCommand command = new InsertCommand(factHandle, entryPoint );
        sender.sendCommand(command, envConfig.getEventsTopicName());
    }

}