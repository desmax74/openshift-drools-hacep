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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.kie.remote.RemoteCepEntryPoint;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.impl.RemoteFactHandleImpl;
import org.kie.u212.consumer.Listener;

public class RemoteCepEntryPointImpl implements RemoteCepEntryPoint {

    protected final Sender sender;
    protected final Listener listener;
    private ExecutorService executor;
    private final String entryPoint;

    public RemoteCepEntryPointImpl(Sender sender, String entryPoint ) {
        this.sender = sender;
        this.entryPoint = entryPoint;
        this.listener = new Listener();
        this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public String getEntryPointId() {
        return entryPoint;
    }

    @Override
    public void insert(Object object) {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl(object );
        InsertCommand command = new InsertCommand(factHandle, entryPoint );
        sender.sendCommand(command);
    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects() {
        return null;
    }

    @Override
    public CompletableFuture<Collection<? extends Object>> getObjects(Predicate<Object> filter) {
        return null;
    }

    @Override
    public CompletableFuture<Long> getFactCount() {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl();
        FactCountCommand command = new FactCountCommand(factHandle, entryPoint );
        sender.sendCommand(command);
        return CompletableFuture.supplyAsync(() -> (listener.getFactCount(factHandle).getFactCount()), executor);
    }
}
