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
package org.kie.hacep.core.infra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.api.KieServices;
import org.kie.api.marshalling.KieMarshallers;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.hacep.Config;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.KieSessionContext;
import org.kie.hacep.message.SnapshotMessage;
import org.kie.remote.impl.producer.EventProducer;
import org.kie.remote.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSessionSnapShooter implements SessionSnapshooter {

    private final String key = "LAST-SNAPSHOT";
    private final Logger logger = LoggerFactory.getLogger(DefaultSessionSnapShooter.class);
    private KieContainer kieContainer;
    private EnvConfig envConfig;

    public DefaultSessionSnapShooter(EnvConfig envConfig) {
        this.envConfig = envConfig;
        KieServices srv = KieServices.get();
        if (srv != null) {
            kieContainer = srv.newKieClasspathContainer();
        } else {
            logger.error("KieServices is null");
        }
    }

    public void serialize(KieSessionContext kieSessionContext,
                          String lastInsertedEventkey,
                          long lastInsertedEventOffset) {
        KieMarshallers marshallers = KieServices.get().getMarshallers();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            EventProducer<byte[]> producer = new EventProducer<>();
            producer.start(Config.getSnapshotProducerConfig());
            marshallers.newMarshaller(kieSessionContext.getKieSession().getKieBase()).marshall(out,
                                                                                               kieSessionContext.getKieSession());
            /* We are storing the last inserted key and offset together with the session's bytes */
            byte[] bytes = out.toByteArray();
            SnapshotMessage message = new SnapshotMessage(UUID.randomUUID().toString(),
                                                          bytes,
                                                          kieSessionContext.getFhManager(),
                                                          lastInsertedEventkey,
                                                          lastInsertedEventOffset,
                                                          LocalDateTime.now());
            producer.produceSync(envConfig.getSnapshotTopicName(),
                                 key,
                                 message);
            producer.stop();
        } catch (IOException e) {
            logger.error(e.getMessage(),
                         e);
        }
    }

    public SnapshotInfos deserialize() {
        KieServices srv = KieServices.get();
        if (srv != null) {
            KafkaConsumer<String, byte[]> consumer = getConfiguredSnapshotConsumer();
            KieMarshallers marshallers = KieServices.get().getMarshallers();
            KieSession kSession = null;
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.of(Integer.valueOf(Config.DEFAULT_POLL_TIMEOUT_MS),
                                                                                ChronoUnit.MILLIS));
            byte[] bytes = null;
            for (ConsumerRecord record : records) {
                bytes = (byte[]) record.value();
            }
            consumer.close();
            SnapshotMessage snapshotMsg = bytes != null ? SerializationUtil.deserialize(bytes) : null;

            if (snapshotMsg != null) {
                try (ByteArrayInputStream in = new ByteArrayInputStream(snapshotMsg.getSerializedSession())) {
                    KieSessionConfiguration conf = KieServices.get().newKieSessionConfiguration();
                    conf.setOption(ClockTypeOption.get("pseudo"));
                    kSession = marshallers.newMarshaller(kieContainer.getKieBase()).unmarshall(in,
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
        }
        return null;
    }

    private KafkaConsumer getConfiguredSnapshotConsumer() {
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

    @Override
    public LocalDateTime getLastSnapshotTime() {
        KafkaConsumer<String, byte[]> consumer = getConfiguredSnapshotConsumer();
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.of(Integer.valueOf(Config.DEFAULT_POLL_TIMEOUT_MS),
                                                                            ChronoUnit.MILLIS));
        byte[] bytes = null;
        for (ConsumerRecord record : records) {
            bytes = (byte[]) record.value();
        }
        consumer.close();
        SnapshotMessage snapshotMsg = bytes != null ? SerializationUtil.deserialize(bytes) : null;
        if (snapshotMsg != null) {
            return snapshotMsg.getTime();
        } else {
            return null;
        }
    }
}
