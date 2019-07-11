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
package org.kie.remote;

import java.util.Optional;

import org.kie.remote.util.PrinterLogImpl;

public final class EnvConfig {

    private String namespace;
    private String eventsTopicName;
    private String controlTopicName;
    private String snapshotTopicName;
    private String kieSessionInfosTopicName;
    private String printerType;

    public static EnvConfig getDefaultEnvConfig(){
        return anEnvConfig().
                withNamespace(Optional.ofNullable(System.getenv( Config.NAMESPACE)).orElse(Config.DEFAULT_NAMESPACE)).
                withControlTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_CONTROL_TOPIC)).orElse(Config.DEFAULT_CONTROL_TOPIC)).
                withEventsTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_EVENTS_TOPIC)).orElse(Config.DEFAULT_EVENTS_TOPIC)).
                withSnapshotTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_SNAPSHOT_TOPIC)).orElse(Config.DEFAULT_SNAPSHOT_TOPIC)).
                withKieSessionInfosTopicName(Optional.ofNullable(System.getenv(Config.DEFAULT_KIE_SESSION_INFOS_TOPIC)).orElse(Config.DEFAULT_KIE_SESSION_INFOS_TOPIC)).
                withPrinterType(Optional.ofNullable(System.getenv(Config.DEFAULT_PRINTER_TYPE)).orElse(PrinterLogImpl.class.getName())).build();
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

    public EnvConfig withPrinterType(String printerType) {
        this.printerType = printerType;
        return this;
    }

    public EnvConfig build() {
        EnvConfig envConfig = new EnvConfig();
        envConfig.eventsTopicName = this.eventsTopicName;
        envConfig.namespace = this.namespace;
        envConfig.controlTopicName = this.controlTopicName;
        envConfig.snapshotTopicName = this.snapshotTopicName;
        envConfig.kieSessionInfosTopicName = this.kieSessionInfosTopicName;
        envConfig.printerType = this.printerType;
        return envConfig;
    }

    public String getNamespace() { return namespace; }

    public String getEventsTopicName() { return eventsTopicName; }

    public String getControlTopicName() { return controlTopicName; }

    public String getSnapshotTopicName() { return snapshotTopicName; }

    public String getKieSessionInfosTopicName() { return kieSessionInfosTopicName; }

    public String getPrinterType() { return printerType; }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EnvConfig{");
        sb.append("namespace='").append(namespace).append('\'');
        sb.append(", eventsTopicName='").append(eventsTopicName).append('\'');
        sb.append(", controlTopicName='").append(controlTopicName).append('\'');
        sb.append(", snapshotTopicName='").append(snapshotTopicName).append('\'');
        sb.append(", kieSessionInfosTopicName='").append(kieSessionInfosTopicName).append('\'');
        sb.append(", printerType='").append(printerType).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

