/*
 * Copyright 2019 Red Hat
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
package org.kie.remote.impl;

import org.junit.Test;

import static org.junit.Assert.*;

public class RemoteFactHandleTest {

    @Test
    public void defaultConstructorTest(){
        RemoteFactHandleImpl handle = new RemoteFactHandleImpl();
        assertNotNull(handle);
        assertNotNull(handle.getId());
        assertNull(handle.getObject());
    }

    @Test
    public void constructorTest(){
        String mi5 = "005";
        RemoteFactHandleImpl handle = new RemoteFactHandleImpl(mi5);
        assertNotNull(handle);
        assertNotNull(handle.getId());
        assertNotNull(handle.getObject());
    }

    @Test
    public void equalsTest(){
        String mi6 = "006";
        RemoteFactHandleImpl handleMi6 = new RemoteFactHandleImpl(mi6);
        assertNotNull(handleMi6);
        assertNotNull(handleMi6.getId());
        assertNotNull(handleMi6.getObject());
        String mi5 = "005";
        RemoteFactHandleImpl handleMi5 = new RemoteFactHandleImpl(mi5);
        assertFalse(handleMi6.equals(handleMi5));
        assertNotSame(handleMi6.toString(),handleMi5.toString() );
    }

}
