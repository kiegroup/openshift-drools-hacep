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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.Callback;
import org.kie.remote.util.LocalMessageSystem;

public class LocalProducer implements Producer {

    private final LocalMessageSystem queue = LocalMessageSystem.get();

    private final Map<String, CompletableFuture<Object>> results = new HashMap<>();

    @Override
    public void start( Properties properties ) { }

    @Override
    public void stop() { }

    @Override
    public void produceFireAndForget( String topicName, String key, Object object ) {
        queue.put( topicName, object );
    }

    @Override
    public void produceSync( String topicName, String key, Object object ) {
        queue.put( topicName, object );
    }

    @Override
    public void produceAsync( String topicName, String key, Object object, Callback callback ) {
        queue.put( topicName, object );
    }
}
