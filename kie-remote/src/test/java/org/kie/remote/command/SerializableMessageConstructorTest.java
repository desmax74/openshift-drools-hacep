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

import org.junit.Test;
import org.kie.remote.message.ControlMessage;
import org.kie.remote.message.FactCountMessage;
import org.kie.remote.message.FireAllRuleMessage;
import org.kie.remote.message.GetKJarGAVMessage;
import org.kie.remote.message.GetObjectMessage;
import org.kie.remote.message.ListKieSessionObjectMessage;
import org.kie.remote.message.UpdateKJarMessage;

import static org.junit.Assert.*;

public class SerializableMessageConstructorTest {

    @Test
    public void controlMessageTest(){
        ControlMessage msg = new ControlMessage();
        assertTrue(0l == msg.getOffset());
        assertNotNull(msg.toString());
    }

    @Test
    public void factCountMessageTest(){
        FactCountMessage msg = new FactCountMessage();
        assertTrue(0l == msg.getFactCount());
        assertNotNull(msg.toString());
    }

    @Test
    public void fireAllRuleMessageTest(){
        FireAllRuleMessage msg = new FireAllRuleMessage();
        assertTrue(0l == msg.getCounter());
        assertNotNull(msg.toString());
    }

    @Test
    public void getKJarGAVMessageTest(){
        GetKJarGAVMessage msg = new GetKJarGAVMessage();
        assertNull(msg.getkJarGAV());
        assertNotNull(msg.toString());
    }

    @Test
    public void getObjectMessageTest(){
        GetObjectMessage msg = new GetObjectMessage();
        assertNull(msg.getObject());
        assertNotNull(msg.toString());
    }

    @Test
    public void listKieSessionObjectMessageTest(){
        ListKieSessionObjectMessage msg = new ListKieSessionObjectMessage();
        assertNull(msg.getObjects());
        assertNotNull(msg.toString());
    }

    @Test
    public void updateKJarMessageTest(){
        UpdateKJarMessage msg = new UpdateKJarMessage();
        assertNull(msg.getId());
        assertNotNull(msg.toString());
    }
}
