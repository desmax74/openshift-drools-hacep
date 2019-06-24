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

import java.io.Serializable;

public class FactCountMessage implements Serializable, VisitableMessage {

    private String key;
    private long factCount;

    public FactCountMessage(){}

    public FactCountMessage(String key,
                            long factCount) {
        this.key = key;
        this.factCount = factCount;
    }

    public String getKey() {
        return key;
    }

    public long getFactCount() {
        return factCount;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FactCountMessage{");
        sb.append("key='").append(key).append('\'');
        sb.append(", factCount=").append(factCount);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void accept(VisitorMessage visitor) {
        visitor.visit(this, key);
    }
}
