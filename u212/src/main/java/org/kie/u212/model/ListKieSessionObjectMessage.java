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
import java.util.Collection;

public class ListKieSessionObjectMessage implements Serializable {

    private String key;
    private Collection<? extends Object> objects;

    public ListKieSessionObjectMessage(){}

    public ListKieSessionObjectMessage(String key, Collection<? extends Object> objects) {
        this.key = key;
        this.objects = objects;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public Collection<? extends Object> getObjects() {
        return objects;
    }

    public void setObjects(Collection<? extends Object> objects) {
        this.objects = objects;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ListKieSessionObjectMessage{");
        sb.append("key='").append(key).append('\'');
        sb.append(", objects=").append(objects);
        sb.append('}');
        return sb.toString();
    }
}
