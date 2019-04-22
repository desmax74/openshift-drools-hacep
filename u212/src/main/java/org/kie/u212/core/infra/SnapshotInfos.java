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
package org.kie.u212.core.infra;

import org.kie.api.runtime.KieSession;

public class SnapshotInfos {

    private KieSession kieSession;
    private String keyDuringSnaphot;
    private long offsetDuringSnapshot;

    public SnapshotInfos() {
    }

    public SnapshotInfos(KieSession kieSession,
                         String keyDuringSnaphot,
                         long offsetDuringSnapshot) {
        this.kieSession = kieSession;
        this.keyDuringSnaphot = keyDuringSnaphot;
        this.offsetDuringSnapshot = offsetDuringSnapshot;
    }

    public KieSession getKieSession() {
        return kieSession;
    }

    public String getKeyDuringSnaphot() {
        return keyDuringSnaphot;
    }

    public long getOffsetDuringSnapshot() {
        return offsetDuringSnapshot;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SnapshotInfos{");
        sb.append("kieSession=").append(kieSession);
        sb.append(", keyDuringSnaphot='").append(keyDuringSnaphot).append('\'');
        sb.append(", offsetDuringSnapshot=").append(offsetDuringSnapshot);
        sb.append('}');
        return sb.toString();
    }
}
