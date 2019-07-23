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
import java.util.concurrent.ConcurrentHashMap;

import org.kie.remote.RemoteCepEntryPoint;
import org.kie.remote.RemoteCepKieSession;
import org.kie.remote.TopicsConfig;

import static org.kie.remote.impl.producer.RemoteKieSessionImpl.DEFAULT_ENTRY_POINT;

public class RemoteCepKieSessionImpl extends RemoteCepEntryPointImpl implements Closeable,
                                                                                RemoteCepKieSession {

    public RemoteCepKieSessionImpl(Properties configuration, TopicsConfig envConfig) {
        super(new Sender(configuration), DEFAULT_ENTRY_POINT, envConfig, new ConcurrentHashMap<>());
        sender.start();
    }

    @Override
    public void close() {
        sender.stop();
        requestsStore.clear();
    }

    @Override
    public RemoteCepEntryPoint getEntryPoint(String name ) {
        return getEntryPoint(name);
    }

}
