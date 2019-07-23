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
package org.kie.remote;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonConfig {

    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);
    public static final String DEFAULT_NAMESPACE = "default";
    public static final String DEFAULT_EVENTS_TOPIC = "events";
    public static final String DEFAULT_KIE_SESSION_INFOS_TOPIC = "kiesessioninfos";
    public static final int DEFAULT_POLL_TIMEOUT_MS = 1000;

    public static final String KEY_SERIALIZER_KEY = "key.serializer";
    public static final String VALUE_SERIALIZER_KEY = "value.serializer";
    public static final String KEY_DESERIALIZER_KEY = "key.deserializer";
    public static final String VALUE_DESERIALIZER_KEY = "value.deserializer";
    private static Properties consumerConf, producerConf;
    private static final String CONSUMER_CONF = "consumer.properties";
    private static final String PRODUCER_CONF = "producer.properties";

    private static Properties config;

    public static Properties getStatic() {
        if(config == null) {
            config = new Properties();
            config.put(KEY_SERIALIZER_KEY, "org.apache.kafka.common.serialization.StringSerializer");
            config.put(VALUE_SERIALIZER_KEY, "org.apache.kafka.common.serialization.ByteArraySerializer");
            config.put(KEY_DESERIALIZER_KEY, "org.apache.kafka.common.serialization.StringDeserializer");
            config.put(VALUE_DESERIALIZER_KEY, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }
        return config;
    }

    public static Properties getProducerConfig(String caller) {
        if(producerConf == null){
            producerConf = getDefaultConfigFromProps(PRODUCER_CONF);
        }
        logConfig(caller,producerConf);
        return producerConf;
    }

    public static Properties getDefaultConfigFromProps(String fileName) {
        Properties config = new Properties();
        InputStream in = null;
        try {
            in = CommonConfig.class.getClassLoader().getResourceAsStream(fileName);
        } catch (Exception e) {
        } finally {
            try {
                config.load(in);
                in.close();
            } catch (IOException ioe) {
                logger.error(ioe.getMessage(),
                             ioe);
            }
        }
        return config;
    }

    private static void logConfig(String subject,Properties producerProperties) {
        if (logger.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append(subject);
            sb.append("\n{\n");
            for (Map.Entry<Object, Object> entry : producerProperties.entrySet()) {
                sb.append(" ").append(entry.getKey().toString()).append(":").append(entry.getValue()).append("  \n");
            }
            sb.append("\n}\n");
            logger.info(sb.toString());
        }
    }
}
