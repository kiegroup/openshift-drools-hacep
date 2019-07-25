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
package org.kie.hacep.message;

import java.io.Serializable;
import java.util.Queue;

import org.kie.remote.message.Message;

public class ControlMessage implements Serializable, Message {

    private String key;
    private long offset;
    private long timestamp;
    private Queue<Object> sideEffects;

    /* Empty constructor for serialization */
    public ControlMessage() {}

    public ControlMessage( String key,
                           Queue<Object> sideEffects) {
        this.key = key;
        this.sideEffects = sideEffects;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String getId() {
        return getKey();
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Queue<Object> getSideEffects() {
        return sideEffects;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ControlMessage{");
        sb.append(", key='").append(key).append('\'');
        if(offset != 0l) {
            sb.append(", offset=").append(offset);
        }
        if(timestamp != 0l) {
            sb.append(", timestamp=").append(timestamp);
        }
        if(sideEffects != null && !sideEffects.isEmpty()){
            sb.append(", sideEffects=").append("\n").append(sideEffects);
        }
        sb.append('}');
        return sb.toString();
    }
}
