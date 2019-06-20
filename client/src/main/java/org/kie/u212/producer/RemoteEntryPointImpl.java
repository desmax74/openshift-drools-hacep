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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.kie.remote.RemoteEntryPoint;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.DeleteCommand;
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.UpdateCommand;
import org.kie.remote.impl.RemoteFactHandleImpl;
import org.kie.u212.consumer.Listener;

public class RemoteEntryPointImpl implements RemoteEntryPoint {

    protected final Sender sender;
    protected final Listener listener;
    private ExecutorService executor;
    private final String entryPoint;
    private Map<String, Object> requestsStore;

    public RemoteEntryPointImpl(Sender sender, String entryPoint ) {
        this.sender = sender;
        this.entryPoint = entryPoint;
        requestsStore = new ConcurrentHashMap<>();
        this.listener = new Listener(requestsStore);
        this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public String getEntryPointId() {
        return entryPoint;
    }

    @Override
    public RemoteFactHandle insert(Object obj) {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl( obj );
        InsertCommand command = new InsertCommand(factHandle, entryPoint );
        sender.sendCommand(command);
        return factHandle;
    }

    @Override
    public void delete( RemoteFactHandle handle ) {
        DeleteCommand command = new DeleteCommand(handle, entryPoint );
        sender.sendCommand(command);
    }

    @Override
    public void update( RemoteFactHandle handle, Object object ) {
        UpdateCommand command = new UpdateCommand(handle, object, entryPoint);
        sender.sendCommand(command);
    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects() {
        throw new UnsupportedOperationException( "org.kie.u212.producer.RemoteKieSessionImpl.getObjects -> TODO" );

    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects( Predicate<Object> filter ) {
        throw new UnsupportedOperationException( "org.kie.u212.producer.RemoteKieSessionImpl.getObjects -> TODO" );

    }

    @Override
    public CompletableFuture<Long> getFactCount() {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl();
        FactCountCommand command = new FactCountCommand(factHandle, entryPoint );
        sender.sendCommand(command);
        listener.retrieveAndStoreFactCount(factHandle);
        return CompletableFuture.supplyAsync(() -> ((Long)requestsStore.get(factHandle.getId())), executor);
    }
}