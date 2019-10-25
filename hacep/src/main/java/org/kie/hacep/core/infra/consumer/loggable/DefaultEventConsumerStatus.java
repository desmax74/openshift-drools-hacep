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
package org.kie.hacep.core.infra.consumer.loggable;

import org.kie.hacep.core.infra.election.State;

public class DefaultEventConsumerStatus implements EventConsumerStatus {

    private volatile State currentState = State.REPLICA;
    private volatile PolledTopic polledTopic = PolledTopic.CONTROL;
    private volatile boolean started, exit = false;
    private volatile boolean askedSnapshotOnDemand;
    private volatile long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;
    private volatile String processingKey = "";

    @Override
    public long getProcessingKeyOffset() {
        return processingKeyOffset;
    }

    @Override
    public void setProcessingKeyOffset(long processingKeyOffset) {
        this.processingKeyOffset = processingKeyOffset;
    }

    @Override
    public long getLastProcessedControlOffset() {
        return lastProcessedControlOffset;
    }

    @Override
    public void setLastProcessedControlOffset(long lastProcessedControlOffset) {
        this.lastProcessedControlOffset = lastProcessedControlOffset;
    }

    @Override
    public long getLastProcessedEventOffset() {
        return lastProcessedEventOffset;
    }

    @Override
    public void setLastProcessedEventOffset(long lastProcessedEventOffset) {
        this.lastProcessedEventOffset = lastProcessedEventOffset;
    }

    @Override
    public String getProcessingKey() {
        return processingKey;
    }

    @Override
    public void setProcessingKey(String processingKey) {
        this.processingKey = processingKey;
    }

    @Override
    public State getCurrentState() {
        return currentState;
    }

    @Override
    public void setCurrentState(State currentState) {
        this.currentState = currentState;
    }

    @Override
    public PolledTopic getPolledTopic() {
        return polledTopic;
    }

    @Override
    public void setPolledTopic(PolledTopic polledTopic) {
        this.polledTopic = polledTopic;
    }

    @Override
    public boolean isExit() {
        return exit;
    }

    @Override
    public void setExit(boolean exit) {
        this.exit = exit;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public void setStarted(boolean started) {
        this.started = started;
    }

    @Override
    public boolean isAskedSnapshotOnDemand() {
        return askedSnapshotOnDemand;
    }

    @Override
    public void setAskedSnapshotOnDemand(boolean askedSnapshotOnDemand) {
        this.askedSnapshotOnDemand = askedSnapshotOnDemand;
    }

    public enum PolledTopic {
        EVENTS,
        CONTROL;
    }
}
