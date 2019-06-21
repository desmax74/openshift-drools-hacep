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
package org.kie.u212.producer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.kie.remote.RemoteEntryPoint;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.DeleteCommand;
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.UpdateCommand;
import org.kie.remote.impl.RemoteFactHandleImpl;
import org.kie.u212.EnvConfig;
import org.kie.u212.consumer.Listener;

public class RemoteEntryPointImpl<T> implements RemoteEntryPoint {

    protected final Sender sender;
    protected final Listener listener;
    private final String entryPoint;
    private Map<String, CompletableFuture<T>> requestsStore;
    private EnvConfig envConfig;

    public RemoteEntryPointImpl(Sender sender, String entryPoint, EnvConfig envConfig) {
        requestsStore = new ConcurrentHashMap<>();
        this.listener = new Listener(requestsStore);
        this.sender = sender;
        this.entryPoint = entryPoint;
        this.envConfig = envConfig;

    }

    public void listen(){
        listener.listen();
    }

    public void stop(){
        listener.stopConsumeEvents();
    }

    @Override
    public String getEntryPointId() {
        return entryPoint;
    }

    @Override
    public RemoteFactHandle insert(Object obj) {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl( obj );
        InsertCommand command = new InsertCommand(factHandle, entryPoint );
        sender.sendCommand(command, envConfig.getEventsTopicName());
        return factHandle;
    }

    @Override
    public void delete( RemoteFactHandle handle ) {
        DeleteCommand command = new DeleteCommand(handle, entryPoint );
        sender.sendCommand(command, envConfig.getEventsTopicName());
    }

    @Override
    public void update( RemoteFactHandle handle, Object object ) {
        UpdateCommand command = new UpdateCommand(handle, object, entryPoint);
        sender.sendCommand(command, envConfig.getEventsTopicName());
    }

    @Override
    public void getObjects(CompletableFuture callback) {
        throw new UnsupportedOperationException( "org.kie.u212.producer.RemoteKieSessionImpl.getObjects -> TODO" );
    }

    @Override
    public void getObjects(CompletableFuture callback,
                           Predicate filter) {
        throw new UnsupportedOperationException( "org.kie.u212.producer.RemoteKieSessionImpl.getObjects -> TODO" );
    }

    @Override
    public void getFactCount(CompletableFuture callback) {
        FactCountCommand command = new FactCountCommand(createStoreAndGetRemoteFactHandle(callback), entryPoint );
        sender.sendCommand(command, envConfig.getEventsTopicName());
    }

    private RemoteFactHandle createStoreAndGetRemoteFactHandle(CompletableFuture<T> callback){
        RemoteFactHandle factHandle = new RemoteFactHandleImpl();
        requestsStore.put(factHandle.getId(), callback);
        return factHandle;
    }

}