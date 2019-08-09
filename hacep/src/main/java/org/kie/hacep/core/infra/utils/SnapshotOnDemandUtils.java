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
package org.kie.hacep.core.infra.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.hacep.Config;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.GlobalStatus;
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.message.SnapshotMessage;
import org.kie.remote.TopicsConfig;
import org.kie.remote.command.SnapshotOnDemandCommand;
import org.kie.remote.impl.producer.Sender;
import org.kie.remote.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotOnDemandUtils {

    private final static Logger logger = LoggerFactory.getLogger(SnapshotOnDemandUtils.class);

    public static SnapshotInfos askASnapshotOnDemand(EnvConfig config,
                                                     SessionSnapshooter snapshooter) {
        LocalDateTime infosTime = snapshooter.getLastSnapshotTime();

        LocalDateTime limitAge = LocalDateTime.now().minusSeconds(config.getMaxSnapshotAge());
        if (infosTime != null && limitAge.isBefore(infosTime)) { //included in the max age
            if (logger.isInfoEnabled()) {
                logger.info("Deserialize a recent snapshot");
            }
            return snapshooter.deserialize();
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("Build NewSnapshotOnDemand ");
            }
            return buildNewSnapshotOnDemand(config, limitAge);
        }
    }

    private static SnapshotInfos buildNewSnapshotOnDemand(EnvConfig config,
                                                          LocalDateTime limitAge) {
        SnapshotMessage snapshotMsg = askAndReadSnapshotOnDemand(config,
                                                                 limitAge);
        KieSession kSession = null;
        try (ByteArrayInputStream in = new ByteArrayInputStream(snapshotMsg.getSerializedSession())) {
            KieSessionConfiguration conf = KieServices.get().newKieSessionConfiguration();
            conf.setOption(ClockTypeOption.get("pseudo"));
            kSession = KieServices.get().getMarshallers().newMarshaller(getKieContainer().getKieBase()).unmarshall(in,
                                                                                                                   conf,
                                                                                                                   null);
        } catch (IOException | ClassNotFoundException e) {
            logger.error(e.getMessage(),
                         e);
        }
        return new SnapshotInfos(kSession,
                                 snapshotMsg.getFhManager(),
                                 snapshotMsg.getLastInsertedEventkey(),
                                 snapshotMsg.getLastInsertedEventOffset(),
                                 snapshotMsg.getTime());
    }

    private static KieContainer getKieContainer() {
        KieServices srv = KieServices.get();
        if (srv != null) {
            return srv.newKieClasspathContainer();
        } else {
            throw new RuntimeException("KieServices is null");
        }
    }

    private static SnapshotMessage askAndReadSnapshotOnDemand(EnvConfig envConfig,
                                                              LocalDateTime limitAge) {
        Properties props = Config.getProducerConfig("SnapshotOnDemandUtils.askASnapshotOnDemand");
        Sender sender = new Sender(props);
        sender.start();
        sender.sendCommand(new SnapshotOnDemandCommand(),
                           TopicsConfig.getDefaultTopicsConfig().getEventsTopicName());
        sender.stop();
        KafkaConsumer consumer = getConfiguredSnapshotConsumer(envConfig);
        boolean snapshotReady = false;
        SnapshotMessage msg = null;
        try {
            GlobalStatus.canBecomeLeader = false;
            int counter = 0;
            while (!snapshotReady) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.of(Integer.valueOf(Config.DEFAULT_POLL_TIMEOUT_MS),
                                                                                    ChronoUnit.MILLIS));
                byte[] bytes = null;
                for (ConsumerRecord record : records) {
                    bytes = (byte[]) record.value();
                }
                SnapshotMessage snapshotMsg = bytes != null ? SerializationUtil.deserialize(bytes) : null;
                if (snapshotMsg != null && limitAge.isBefore(snapshotMsg.getTime())) {
                    snapshotReady = true;
                    msg = snapshotMsg;
                }
                else {
                    // use a counter to avoid infinite attempts
                    counter += 1;
                    if(counter > envConfig.getMaxSnapshotRequestAttempts()) {
                        GlobalStatus.nodeLive = false;
                        String errorMessage = "Impossible to retrieve a snapshot and start after " + counter + " attempts";
                        logger.error(errorMessage);
                        throw new IllegalStateException(errorMessage);
                    }
                }
            }
        } finally {
            consumer.close();
            GlobalStatus.canBecomeLeader = true;
        }
        return msg;
    }

    private static KafkaConsumer getConfiguredSnapshotConsumer(EnvConfig envConfig) {
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(Config.getSnapshotConsumerConfig());
        List<PartitionInfo> partitionsInfo = consumer.partitionsFor(envConfig.getSnapshotTopicName());
        List<TopicPartition> partitions = null;
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                if (partitions == null || partitions.contains(partition.partition())) {
                    partitionCollection.add(new TopicPartition(partition.topic(),
                                                               partition.partition()));
                }
            }
            if (!partitionCollection.isEmpty()) {
                consumer.assign(partitionCollection);
            }
        }
        consumer.assignment().forEach(topicPartition -> consumer.seekToBeginning(partitionCollection));
        return consumer;
    }
}
