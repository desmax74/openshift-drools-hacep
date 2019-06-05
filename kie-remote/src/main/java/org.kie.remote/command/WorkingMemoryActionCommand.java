/*
 * Copyright 2005 JBoss Inc
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

import java.util.UUID;

import org.kie.remote.RemoteFactHandle;

public abstract class WorkingMemoryActionCommand extends AbstractCommand {
    private RemoteFactHandle factHandle;
    private String entryPoint;

    /* Empty constructor for serialization */
    public WorkingMemoryActionCommand() { }

    public WorkingMemoryActionCommand( RemoteFactHandle factHandle, String entryPoint ) {
        super( UUID.randomUUID().toString() );
        this.factHandle = factHandle;
        this.entryPoint = entryPoint;
    }

    public RemoteFactHandle getFactHandle() {
        return factHandle;
    }

    public String getEntryPoint() {
        return entryPoint;
    }
}
