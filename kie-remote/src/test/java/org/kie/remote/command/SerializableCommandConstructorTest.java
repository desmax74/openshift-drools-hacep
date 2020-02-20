/*
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
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

import java.io.Serializable;

import org.junit.Test;
import org.kie.remote.impl.RemoteFactHandleImpl;

import static org.junit.Assert.*;

public class SerializableCommandConstructorTest {

    @Test
    public void deleteCommandDefaultTest(){
        DeleteCommand command = new DeleteCommand();
        assertTrue(command.isPermittedForReplicas());
    }

    @Test
    public void deleteCommandTest(){
        DeleteCommand command = new DeleteCommand(new RemoteFactHandleImpl(new Object()), "DEFAULT");
        assertTrue(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void eventInsertCommandDefaultTest(){
        EventInsertCommand command = new EventInsertCommand();
        assertTrue(command.isPermittedForReplicas());
    }

    @Test
    public void eventInsertCommandTest(){
        EventInsertCommand command = new EventInsertCommand(new RemoteFactHandleImpl(new Object()), "DEFAULT");
        assertTrue(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void factCountCommandDefaultTest(){
        FactCountCommand command = new FactCountCommand();
        assertFalse(command.isPermittedForReplicas());
    }

    @Test
    public void factCountCommandTest(){
        FactCountCommand command = new FactCountCommand( "DEFAULT");
        assertFalse(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void fireAllRulesCommandTest(){
        FireAllRulesCommand command = new FireAllRulesCommand();
        assertTrue(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void fireUntilHaltCommandTest(){
        FireUntilHaltCommand command = new FireUntilHaltCommand();
        assertTrue(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void getKJarGAVCommandDefaultTest(){
        GetKJarGAVCommand command = new GetKJarGAVCommand();
        assertFalse(command.isPermittedForReplicas());
    }

    @Test
    public void getKJarGAVCommandTest(){
        GetKJarGAVCommand command = new GetKJarGAVCommand("DEFAULT");
        assertFalse(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void getObjectCommandDefaultTest(){
        GetObjectCommand command = new GetObjectCommand();
        assertFalse(command.isPermittedForReplicas());
    }

    @Test
    public void getObjectCommandTest(){
        GetObjectCommand command = new GetObjectCommand(new RemoteFactHandleImpl(new Object()));
        assertFalse(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void getWorkingSnapshotOnDemandCommandTest(){
        SnapshotOnDemandCommand command = new SnapshotOnDemandCommand();
        assertFalse(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }

    @Test
    public void getUpdateCommandDefaultTest(){
        UpdateCommand command = new UpdateCommand();
        assertTrue(command.isPermittedForReplicas());
    }

    @Test
    public void getUpdateCommandTest(){
        UpdateCommand command = new UpdateCommand(new RemoteFactHandleImpl(new Object()),
                                                  new Serializable() {
                                                      @Override
                                                      public int hashCode() {
                                                          return super.hashCode();
                                                      }
                                                  },
                                                  "DEFAULT");
        assertTrue(command.isPermittedForReplicas());
        assertNotNull(command.toString());
    }


}
