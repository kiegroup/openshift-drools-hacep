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

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.SnapshotMessage;
import org.kie.remote.CommonConfig;

import static org.junit.Assert.*;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class SnapshotOnDemandTest {

    private KafkaUtilTest kafkaServerTest;
    private EnvConfig config;

    public static EnvConfig getEnvConfig() {
        return EnvConfig.anEnvConfig().
                withNamespace(CommonConfig.DEFAULT_NAMESPACE).
                withControlTopicName(Config.DEFAULT_CONTROL_TOPIC).
                withEventsTopicName(CommonConfig.DEFAULT_EVENTS_TOPIC).
                withSnapshotTopicName(Config.DEFAULT_SNAPSHOT_TOPIC).
                withKieSessionInfosTopicName(CommonConfig.DEFAULT_KIE_SESSION_INFOS_TOPIC).
                withPrinterType(PrinterKafkaImpl.class.getName()).
                withPollTimeout("10").
                withMaxSnapshotAgeSeconds("60000").
                isUnderTest(Boolean.TRUE.toString()).build();
    }

    @Before
    public void setUp() throws Exception {
        config = getEnvConfig();
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopics(config.getEventsTopicName(),
                                     config.getControlTopicName(),
                                     config.getSnapshotTopicName(),
                                     config.getKieSessionInfosTopicName()
        );
    }

    @After
    public void tearDown() {
        try {
            Bootstrap.stopEngine();
        } catch (ConcurrentModificationException ex) {
        }
        kafkaServerTest.shutdownServer();
    }

    @Test(timeout = 30000L)
    public void createSnapshotOnDemandTest() {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);

        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("SnapshotOnDemandTest.createSnapshotOnDemandTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    config.getControlTopicName(),
                                                                    Config.getConsumerConfig("SnapshotOnDemandTest.createSnapshotOnDemandTest"));

        KafkaConsumer snapshotConsumer = kafkaServerTest.getConsumer("",
                                                                     config.getSnapshotTopicName(),
                                                                     Config.getConsumerConfig("SnapshotOnDemandTest.createSnapshotOnDemandTest"));

        try {
            ConsumerRecords eventsRecords = eventsConsumer.poll(1000);
            assertEquals(0, eventsRecords.count());

            ConsumerRecords controlRecords = controlConsumer.poll(1000);
            assertEquals(0, controlRecords.count());

            ConsumerRecords snapshotRecords = snapshotConsumer.poll(1000);
            assertEquals(0, snapshotRecords.count());

            KafkaUtilTest.insertSnapshotOnDemandCommand();

            List<SnapshotMessage> messages = new ArrayList<>();
            while (messages.size() < 1) {
                snapshotRecords = snapshotConsumer.poll(5000);
                Iterator<ConsumerRecord<String, byte[]>> snapshotRecordIterator = snapshotRecords.iterator();
                if (snapshotRecordIterator.hasNext()) {
                    ConsumerRecord<String, byte[]> controlRecord = snapshotRecordIterator.next();
                    SnapshotMessage snapshotMessage = deserialize(controlRecord.value());
                    messages.add(snapshotMessage);
                }
            }

            assertEquals(1, messages.size());
            Iterator<SnapshotMessage> messagesIter = messages.iterator();
            SnapshotMessage msg = messagesIter.next();
            assertNotNull(msg);
            assertTrue(msg.getFhManager().getFhMapKeys().isEmpty());
            assertEquals(0, msg.getLastInsertedEventOffset());
            assertNotNull(msg.getSerializedSession());

            eventsRecords = eventsConsumer.poll(1000);
            assertEquals(1, eventsRecords.count());

            controlRecords = controlConsumer.poll(1000);
            assertEquals(1, controlRecords.count());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
            snapshotConsumer.close();
        }
    }
}
