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
package org.kie.u212;

import java.util.Optional;

public final class EnvConfig {

    private String namespace;
    private String eventsTopicName;
    private String controlTopicName;
    private String snapshotTopicName;
    private String kieSessionInfosTopicName;

    public static EnvConfig getDefaultEnvConfig(){
        return anEnvConfig().
                withNamespace(Optional.ofNullable(System.getenv(Config.NAMESPACE)).orElse(Config.DEFAULT_NAMESPACE)).
                withControlTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_CONTROL_TOPIC)).orElse(Config.DEFAULT_CONTROL_TOPIC)).
                withEventsTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_EVENTS_TOPIC)).orElse(Config.DEFAULT_EVENTS_TOPIC)).
                withSnapshotTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_SNAPSHOT_TOPIC)).orElse(Config.DEFAULT_SNAPSHOT_TOPIC)).
                withKieSessionInfosTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_KIE_SESSION_INFOS_TOPIC)).orElse(Config.DEFAULT_KIE_SESSION_INFOS_TOPIC)).build();
    }

    private EnvConfig() { }

    public static EnvConfig anEnvConfig() { return new EnvConfig(); }

    public EnvConfig withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public EnvConfig withEventsTopicName(String eventsTopicName) {
        this.eventsTopicName = eventsTopicName;
        return this;
    }

    public EnvConfig withControlTopicName(String controlTopicName) {
        this.controlTopicName = controlTopicName;
        return this;
    }

    public EnvConfig withSnapshotTopicName(String snapshotTopicName) {
        this.snapshotTopicName = snapshotTopicName;
        return this;
    }

    public EnvConfig withKieSessionInfosTopicName(String kieSessionInfosTopicName) {
        this.kieSessionInfosTopicName = kieSessionInfosTopicName;
        return this;
    }

    public EnvConfig build() {
        EnvConfig envConfig = new EnvConfig();
        envConfig.eventsTopicName = this.eventsTopicName;
        envConfig.namespace = this.namespace;
        envConfig.controlTopicName = this.controlTopicName;
        envConfig.snapshotTopicName = this.snapshotTopicName;
        envConfig.kieSessionInfosTopicName = this.kieSessionInfosTopicName;
        return envConfig;
    }

    public String getNamespace() { return namespace; }

    public String getEventsTopicName() { return eventsTopicName; }

    public String getControlTopicName() { return controlTopicName; }

    public String getSnapshotTopicName() { return snapshotTopicName; }

    public String getKieSessionInfosTopicName() { return kieSessionInfosTopicName; }
}

