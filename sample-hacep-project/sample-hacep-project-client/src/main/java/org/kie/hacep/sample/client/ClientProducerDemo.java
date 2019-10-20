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
package org.kie.hacep.sample.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.CommonConfig;
import org.kie.remote.RemoteStreamingKieSession;
import org.kie.remote.TopicsConfig;

public class ClientProducerDemo {

    public static void main(String[] args) {
        try {
            insertBatchEvent(1);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void insertBatchEvent(int items) throws IOException {
        TopicsConfig envConfig = TopicsConfig.getDefaultTopicsConfig();
        Properties props = getProperties();
        try (RemoteStreamingKieSession producer = RemoteStreamingKieSession.create(props, envConfig)){
            for (int i = 0; i < items; i++) {
                StockTickEvent eventA = new StockTickEvent("RHT",
                                                           ThreadLocalRandom.current().nextLong(80,
                                                                                                100));
                producer.insert(eventA);
            }
        }
    }

    private static Properties getProperties() throws IOException {
        Properties props = CommonConfig.getStatic();

        try (InputStream is = ClientProducerDemo.class.getClassLoader().getResourceAsStream("configuration.properties")) {
            props.load(is);
        } catch (IOException io) {
            io.printStackTrace();
            throw io;
        }

        return props;
    }
}
