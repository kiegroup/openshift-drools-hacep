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

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;

import static org.kie.remote.CommonConfig.LOCAL_MESSAGE_SYSTEM_CONF;
import static org.kie.remote.util.ConfigurationUtil.readBoolean;

public interface Producer {

    void start(Properties properties);

    void stop();

    void produceFireAndForget(String topicName, String key, Object object);

    void produceSync(String topicName, String key, Object object);

    void produceAsync(String topicName, String key, Object object, Callback callback);

    static Producer get(Properties configuration) {
        return get(readBoolean(configuration, LOCAL_MESSAGE_SYSTEM_CONF));
    }

    static Producer get(boolean isLocal) {
        return isLocal ? new LocalProducer() : new EventProducer();
    }
}
