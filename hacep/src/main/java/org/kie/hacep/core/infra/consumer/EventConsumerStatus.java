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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kie.hacep.core.infra.election.State;

public class EventConsumerStatus {

    private volatile State currentState = State.REPLICA;
    private volatile PolledTopic polledTopic = PolledTopic.CONTROL;
    private volatile boolean started, exit = false;
    private volatile boolean askedSnapshotOnDemand;
    private volatile long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;
    private volatile String processingKey = "";
    /*
    Is still needed ?
    private Map<TopicPartition, OffsetAndMetadata> offsetsEvents = new HashMap<>();

    public Map<TopicPartition, OffsetAndMetadata> getOffsetsEvents() {
        return offsetsEvents;
    }

    public void setOffsetsEvents(Map<TopicPartition, OffsetAndMetadata> offsetsEvents) {
        this.offsetsEvents = offsetsEvents;
    }*/

    public long getProcessingKeyOffset() {
        return processingKeyOffset;
    }

    public void setProcessingKeyOffset(long processingKeyOffset) {
        this.processingKeyOffset = processingKeyOffset;
    }

    public long getLastProcessedControlOffset() {
        return lastProcessedControlOffset;
    }

    public void setLastProcessedControlOffset(long lastProcessedControlOffset) {
        this.lastProcessedControlOffset = lastProcessedControlOffset;
    }

    public long getLastProcessedEventOffset() {
        return lastProcessedEventOffset;
    }

    public void setLastProcessedEventOffset(long lastProcessedEventOffset) {
        this.lastProcessedEventOffset = lastProcessedEventOffset;
    }

    public String getProcessingKey() {
        return processingKey;
    }

    public void setProcessingKey(String processingKey) {
        this.processingKey = processingKey;
    }

    public State getCurrentState() {
        return currentState;
    }

    public void setCurrentState(State currentState) {
        this.currentState = currentState;
    }

    public PolledTopic getPolledTopic() {
        return polledTopic;
    }

    public void setPolledTopic(PolledTopic polledTopic) {
        this.polledTopic = polledTopic;
    }

    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public boolean isAskedSnapshotOnDemand() {
        return askedSnapshotOnDemand;
    }

    public void setAskedSnapshotOnDemand(boolean askedSnapshotOnDemand) {
        this.askedSnapshotOnDemand = askedSnapshotOnDemand;
    }

    public enum PolledTopic {
        EVENTS, CONTROL;
    }
}
