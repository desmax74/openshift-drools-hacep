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
package org.kie.remote.impl.producer;

import java.io.Closeable;
import java.util.Properties;

import org.kie.remote.EnvConfig;
import org.kie.remote.RemoteCepEntryPoint;
import org.kie.remote.RemoteCepKieSession;

import static org.kie.remote.impl.producer.RemoteKieSessionImpl.DEFAULT_ENTRY_POINT;

public class RemoteCepKieSessionImpl extends RemoteCepEntryPointImpl implements Closeable,
                                                                                RemoteCepKieSession {

    private EnvConfig envConfig;

    public RemoteCepKieSessionImpl(Properties configuration, EnvConfig envConfig ) {
        super(new Sender(configuration), DEFAULT_ENTRY_POINT, envConfig);
        sender.start();
        this.envConfig = envConfig;
    }

    @Override
    public void close() {
        sender.stop();
    }

    @Override
    public RemoteCepEntryPoint getEntryPoint(String name ) {
        return new RemoteCepEntryPointImpl(sender, name, envConfig);
    }

}
