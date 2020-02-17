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
package org.kie.hacep.exceptions;

import org.junit.Test;

public class ExceptionsTest {


    @Test(expected = ConfigurationException.class)
    public void ConfigurationExceptionTest(){
        throw new ConfigurationException("This is a test");
    }

    @Test(expected = InitializeException.class)
    public void InitializeExceptionTest(){
        throw new InitializeException("This is a test");
    }

    @Test(expected = ProcessCommandException.class)
    public void ProcessCommandExceptionTest(){
        throw new ProcessCommandException("This is a test", new Throwable("This is a test"));
    }

    @Test(expected = ShutdownException.class)
    public void ShutdownExceptionTest(){
        throw new ShutdownException("This is a test", new Throwable("This is a test"));
    }

    @Test(expected = SnapshotOnDemandException.class)
    public void SnapshotOnDemandExceptionTest(){
        throw new SnapshotOnDemandException("This is a test", new Throwable("This is a test"));
    }

    @Test(expected = UnsupportedStateException.class)
    public void UnsupportedStateExceptionTest(){
        throw new UnsupportedStateException("This is a test");
    }
}
