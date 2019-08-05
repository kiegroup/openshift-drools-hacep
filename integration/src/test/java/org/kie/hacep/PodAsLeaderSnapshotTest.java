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
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.message.SnapshotMessage;
import org.kie.remote.RemoteKieSession;
import org.kie.remote.TopicsConfig;

import static org.junit.Assert.*;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodAsLeaderSnapshotTest {

    private final String TEST_KAFKA_LOGGER_TOPIC = "logs";
    private final String TEST_TOPIC = "test";
    private KafkaUtilTest kafkaServerTest;
    private EnvConfig config;
    private TopicsConfig topicsConfig;


    @Before
    public void setUp() throws Exception {
        config = KafkaUtilTest.getEnvConfig();
        topicsConfig = TopicsConfig.getDefaultTopicsConfig();
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopics(TEST_KAFKA_LOGGER_TOPIC,
                                     TEST_TOPIC,
                                     config.getEventsTopicName(),
                                     config.getControlTopicName(),
                                     config.getSnapshotTopicName(),
                                     config.getKieSessionInfosTopicName()
        );
    }

    @After
    public void tearDown() {
        kafkaServerTest.tearDown();
    }


    @Test(timeout = 30000)
    public void processMessagesAsLeaderAndCreateSnapshotTest() {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer snapshotConsumer = kafkaServerTest.getConsumer("",
                                                                     config.getSnapshotTopicName(),
                                                                     Config.getSnapshotConsumerConfig());
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    config.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlConsumerProcessOneSentMessageAsLeaderTest"));

        kafkaServerTest.insertBatchStockTicketEvent(10,
                                                    topicsConfig,
                                                    RemoteKieSession.class);
        try {

            List<ControlMessage> messages = new ArrayList<>();
            while(messages.size()< 11) {
                ConsumerRecords controlRecords = controlConsumer.poll(5000);
                Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
                if(!controlRecordIterator.hasNext()){continue;}
                ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
                ControlMessage controlMessage = deserialize(controlRecord.value());
                messages.add(controlMessage);
            }

            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(11,
                         eventsRecords.count()); //1 fireUntilHalt + 10 stock ticket

            //SNAPSHOT TOPIC
            ConsumerRecords snapshotRecords = snapshotConsumer.poll(5000);
            assertEquals(1, snapshotRecords.count());
            ConsumerRecord record = (ConsumerRecord) snapshotRecords.iterator().next();
            SnapshotMessage snapshot = deserialize((byte[]) record.value());
            assertNotNull(snapshot);
            assertTrue(snapshot.getLastInsertedEventOffset() > 0);
            assertFalse(snapshot.getFhMapKeys().isEmpty());
            assertNotNull(snapshot.getLastInsertedEventkey());
            assertEquals(9, snapshot.getFhMapKeys().size());
            assertNotNull(snapshot.getLastInsertedEventkey());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            snapshotConsumer.close();
        }
    }
}
