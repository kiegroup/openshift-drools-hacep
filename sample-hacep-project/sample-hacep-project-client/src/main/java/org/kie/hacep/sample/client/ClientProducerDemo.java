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
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.CommonConfig;
import org.kie.remote.RemoteStreamingKieSession;
import org.kie.remote.TopicsConfig;
import org.kie.remote.impl.RemoteKieSessionImpl;

public class ClientProducerDemo {

    public static void main(String[] args) {
        try {
            insertBatchEvent(1);
        }catch (Exception e){
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

    private static Properties getProperties() {
        Properties props = CommonConfig.getStatic();
        props.put("bootstrap.servers", "my-cluster-kafka-bootstrap-my-kafka-project.<ip>.nip.io:443");
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", "/<path>/openshift-drools-hacep/sample-hacep-project/sample-hacep-project-client/src/main/resources/keystore.jks");
        props.put("ssl.keystore.password", "<password>");
        props.put("ssl.truststore.location", "/<path>/openshift-drools-hacep/sample-hacep-project/sample-hacep-project-client/src/main/resources/keystore.jks");
        props.put("ssl.truststore.password", "<password>");
        return props;
    }
}
