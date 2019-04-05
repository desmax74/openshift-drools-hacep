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
package org.kie.u212.model;

public class EventWrapper<T>  {

    private T domainEvent;
    private String key;
    private long offset;
    private EventType eventType;
    private long timestamp;

    public EventWrapper() {
    }

    public EventWrapper(T domainEvent,
                        String key,
                        long offset,
                        EventType eventType) {
        this.domainEvent = domainEvent;
        this.offset = offset;
        this.key = key;
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public T getDomainEvent() {
        return domainEvent;
    }

    public void setDomainEvent(T domainEvent) {
        this.domainEvent = domainEvent;
    }

    public String getKey() {
        return key;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EventWrapper{");
        sb.append("domainEvent=").append(domainEvent);
        sb.append(", key='").append(key).append('\'');
        sb.append(", offset=").append(offset);
        sb.append(", eventType=").append(eventType);
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        return sb.toString();
    }
}
