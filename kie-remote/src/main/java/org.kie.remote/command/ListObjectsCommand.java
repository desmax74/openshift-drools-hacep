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
package org.kie.remote.command;

import org.kie.api.runtime.ObjectFilter;
import org.kie.remote.RemoteFactHandle;

public class ListObjectsCommand extends WorkingMemoryActionCommand implements VisitableCommand {

    private String namedQuery;
    private Class clazzType;

    /* Empty constructor for serialization */
    public ListObjectsCommand() { }

    public ListObjectsCommand(RemoteFactHandle factHandle, String entryPoint) {
        super(factHandle, entryPoint);
    }

    public ListObjectsCommand(RemoteFactHandle factHandle, String entryPoint, String namedQuery) {
        super(factHandle, entryPoint);
        this.namedQuery = namedQuery;
    }

    public ListObjectsCommand(RemoteFactHandle factHandle, String entryPoint, Class clazzType) {
        super(factHandle, entryPoint);
        this.clazzType = clazzType;
    }

    public String getNamedQuery() { return namedQuery; }

    public Class getClazzType() { return clazzType; }

    @Override
    public void accept(VisitorCommand visitor, boolean execute) { visitor.visit(this, execute); }

    @Override
    public boolean isPermittedForReplicas() { return false; }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ListObjectsCommand{");
        sb.append(", namedQuery='").append(namedQuery).append('\'');
        sb.append(", clazzType=").append(clazzType);
        sb.append('}');
        return sb.toString();
    }
}
