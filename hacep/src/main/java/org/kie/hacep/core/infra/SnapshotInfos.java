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
package org.kie.hacep.core.infra;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.kie.api.runtime.KieSession;
import org.kie.hacep.consumer.FactHandlesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotInfos {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotInfos.class);

    private KieSession kieSession;
    private FactHandlesManager fhManager;
    private String keyDuringSnaphot;
    private long offsetDuringSnapshot;
    private LocalDateTime time;

    public SnapshotInfos(KieSession kieSession,
                         FactHandlesManager fhManager,
                         String keyDuringSnaphot,
                         long offsetDuringSnapshot,
                         LocalDateTime time) {
        this.kieSession = kieSession;
        this.fhManager = fhManager.initFromKieSession( kieSession );
        this.keyDuringSnaphot = keyDuringSnaphot;
        this.offsetDuringSnapshot = offsetDuringSnapshot;
        this.time = time;
    }

    public KieSession getKieSession() {
        return kieSession;
    }

    public FactHandlesManager getFhManager() {
        return fhManager;
    }

    public String getKeyDuringSnaphot() {
        return keyDuringSnaphot;
    }

    public long getOffsetDuringSnapshot() {
        return offsetDuringSnapshot;
    }

    public LocalDateTime getTime() {
        return time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SnapshotInfos{");
        sb.append("kieSession=").append(kieSession);
        sb.append(", keyDuringSnaphot='").append(keyDuringSnaphot).append('\'');
        sb.append(", offsetDuringSnapshot=").append(offsetDuringSnapshot);
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }
}
