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
package org.kie.hacep;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.kie.remote.CommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

    public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    public static final String BATCH_SIZE_KEY = "batch.size";
    public static final String ENABLE_AUTOCOMMIT_KEY = "enable.auto.commit";
    public static final String MAX_POLL_INTERVALS_MS_KEY = "max.poll.interval.ms";
    public static final String METADATA_MAX_AGE_MS_KEY ="metadata.max.age.ms";
    public static final String DEFAULT_KAFKA_PORT ="9092";
    public static final String NAMESPACE = "namespace";
    public static final String DEFAULT_CONTROL_TOPIC = "control";
    public static final String DEFAULT_SNAPSHOT_TOPIC = "snapshot";
    public static final String ITERATION_BETWEEN_SNAPSHOT = "iteration.between.snapshot";
    public static final int DEFAULT_ITERATION_BETWEEN_SNAPSHOT = 10;
    public static final String DEFAULT_PRINTER_TYPE = "printer.type";
    public static final String MAX_SNAPSHOT_AGE = "max.snapshot.age";
    public static final String DEFAULT_MAX_SNAPSHOT_AGE_SEC = "600";

    public static final String MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST = "MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST";
    public static final String BROKER_URL = System.getenv(MY_CLUSTER_KAFKA_BOOTSTRAP_SERVICE_HOST);
    public static final int DEFAULT_POLL_TIMEOUT_MS = 1000;
    public static final String POLL_TIMEOUT_MS = "poll.timeout";
    public static final String SKIP_ON_DEMAND_SNAPSHOT = "skip.ondemandsnapshoot";
    public static final String TEST = Boolean.FALSE.toString();
    public static final String UNDER_TEST = "undertest";
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private static Properties config;
    private static Properties consumerConf, producerConf, snapshotConsumerConf, snapshotProducerConf;
    private static final String CONSUMER_CONF = "consumer.properties";
    private static final String PRODUCER_CONF = "producer.properties";
    private static final String CONF = "infra.properties";
    private static final String SNAPSHOT_CONSUMER_CONF = "snapshot_consumer.properties";
    private static final String SNAPSHOT_PRODUCER_CONF = "snapshot_producer.properties";

    public static String getBootStrapServers() {
        StringBuilder sb = new StringBuilder();
        sb.append(Config.BROKER_URL).append(":").append(DEFAULT_KAFKA_PORT);
        //append("my-cluster-kafka-bootstrap.my-kafka-project.svc:9092");//plain
        //.append(",").append("my-cluster-kafka-brokers.my-kafka-project.svc").append(":9093");//tls
        return sb.toString();
    }

    public static Properties getDefaultConfig() {
        return getDefaultConfigFromProps(CONF);
    }

    public static Properties getConsumerConfig(String caller) {
        if(consumerConf == null){
            consumerConf = getDefaultConfigFromProps(CONSUMER_CONF);
        }
        logConfig(caller,consumerConf);
        return consumerConf;
    }

    public static Properties getProducerConfig(String caller) {
        if(producerConf == null){
            producerConf = getDefaultConfigFromProps(PRODUCER_CONF);
        }
        logConfig(caller,producerConf);
        return producerConf;
    }

    public static Properties getSnapshotConsumerConfig() {
        if(snapshotConsumerConf == null){
            snapshotConsumerConf = getDefaultConfigFromProps(SNAPSHOT_CONSUMER_CONF);
        }
        logConfig("SnapshotConsumer",snapshotConsumerConf);
        return snapshotConsumerConf;
    }

    public static Properties getSnapshotProducerConfig() {
        if(snapshotProducerConf == null){
            snapshotProducerConf = getDefaultConfigFromProps(SNAPSHOT_PRODUCER_CONF);
        }
        logConfig("SnapshotProducer",snapshotProducerConf);
        return snapshotProducerConf;
    }

    public static Properties getStatic() {
        if(config == null) {
            config = new Properties();
            config.put(CommonConfig.KEY_SERIALIZER_KEY, "org.apache.kafka.common.serialization.StringSerializer");
            config.put(CommonConfig.VALUE_SERIALIZER_KEY, "org.apache.kafka.common.serialization.ByteArraySerializer");
            config.put(BOOTSTRAP_SERVERS_KEY, getBootStrapServers());
            config.put(CommonConfig.KEY_DESERIALIZER_KEY, "org.apache.kafka.common.serialization.StringDeserializer");
            config.put(CommonConfig.VALUE_DESERIALIZER_KEY, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            config.put(MAX_POLL_INTERVALS_MS_KEY, "10000");//time to discover the new consumer after a changetopic default 5 min 300000
            config.put(BATCH_SIZE_KEY, "16384");
            config.put(ENABLE_AUTOCOMMIT_KEY, "false");
            config.put(METADATA_MAX_AGE_MS_KEY, "10000");
            config.put(ITERATION_BETWEEN_SNAPSHOT, "10");
        }
        return config;
    }



    public static Properties getDefaultConfigFromProps(String fileName) {
            Properties config = new Properties();
            InputStream in = null;
            try {
                in = Config.class.getClassLoader().getResourceAsStream(fileName);
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

        if(config.get(BOOTSTRAP_SERVERS_KEY)== null){
            config.put(BOOTSTRAP_SERVERS_KEY, getBootStrapServers());
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
