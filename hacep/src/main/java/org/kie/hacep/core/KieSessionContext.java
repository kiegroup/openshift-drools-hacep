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
package org.kie.hacep.core;

import java.util.concurrent.TimeUnit;

import org.kie.api.runtime.KieSession;
import org.kie.api.time.SessionClock;
import org.kie.api.time.SessionPseudoClock;
import org.kie.hacep.consumer.FactHandlesManager;
import org.kie.hacep.core.infra.SnapshotInfos;

public class KieSessionContext {

    private KieSession kieSession;

    private SessionPseudoClock clock;

    private FactHandlesManager fhManager;

    public KieSession getKieSession() {
        return kieSession;
    }

    public void initFromSnapshot(SnapshotInfos infos) {
        setKieSession(infos.getKieSession());
        this.fhManager = infos.getFhManager();
    }

    public void init(KieSession newKiession) {
        setKieSession(newKiession);
        this.fhManager = new FactHandlesManager(newKiession);
    }

    private void setKieSession(KieSession kieSession) {
        this.kieSession = kieSession;
        SessionClock clock = kieSession.getSessionClock();
        if (clock instanceof SessionPseudoClock) {
            this.clock = (SessionPseudoClock) clock;
        }
    }

    public FactHandlesManager getFhManager() {
        return fhManager;
    }

    public void setClockAt(long time) {
        if (clock == null) {
            throw new IllegalStateException( "Drools HACEP is not running with a pseudo-clock" );
        }
        clock.advanceTime(time - clock.getCurrentTime(), TimeUnit.MILLISECONDS);
    }
}
