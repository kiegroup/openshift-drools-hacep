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

package org.kie.hacep.message;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;

import org.kie.hacep.consumer.FactHandlesManager;
import org.kie.remote.RemoteFactHandle;

public class SnapshotMessage implements Serializable {
    private byte[] serializedSession;
    private FactHandlesManager fhManager;
    private String lastInsertedEventkey;
    private long lastInsertedEventOffset;
    private LocalDateTime time;

    /* Empty constructor for serialization */
    public SnapshotMessage() { }

    public SnapshotMessage( byte[] serializedSession, FactHandlesManager fhManager, String lastInsertedEventkey, long lastInsertedEventOffset, LocalDateTime time ) {
        this.serializedSession = serializedSession;
        this.fhManager = fhManager;
        this.lastInsertedEventkey = lastInsertedEventkey;
        this.lastInsertedEventOffset = lastInsertedEventOffset;
        this.time = time;
    }

    public byte[] getSerializedSession() {
        return serializedSession;
    }

    public void setSerializedSession( byte[] serializedSession ) {
        this.serializedSession = serializedSession;
    }

    public FactHandlesManager getFhManager() {
        return fhManager;
    }

    public Set<RemoteFactHandle> getFhMapKeys() {
        return fhManager.getFhMapKeys();
    }

    public String getLastInsertedEventkey() {
        return lastInsertedEventkey;
    }

    public void setLastInsertedEventkey( String lastInsertedEventkey ) {
        this.lastInsertedEventkey = lastInsertedEventkey;
    }

    public long getLastInsertedEventOffset() {
        return lastInsertedEventOffset;
    }

    public void setLastInsertedEventOffset( long lastInsertedEventOffset ) {
        this.lastInsertedEventOffset = lastInsertedEventOffset;
    }

    public LocalDateTime getTime() {
        return time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SnapshotMessage{");
        sb.append("serializedSession=bytes[]");
        sb.append(", fhMapKeys=").append(fhManager);
        sb.append(", lastInsertedEventkey='").append(lastInsertedEventkey).append('\'');
        sb.append(", lastInsertedEventOffset=").append(lastInsertedEventOffset);
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }
}
