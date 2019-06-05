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

import org.kie.remote.RemoteFactHandle;

public class DeleteCommand extends WorkingMemoryActionCommand {

    /* Empty constructor for serialization */
    public DeleteCommand() { }

    public DeleteCommand( RemoteFactHandle factHandle, String entryPoint ) {
        super(factHandle, entryPoint);
    }

    @Override
    public String toString() {
        return "Delete of " + getFactHandle() + " from entry-point " + getEntryPoint();
    }
}
