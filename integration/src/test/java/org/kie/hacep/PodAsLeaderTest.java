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
import java.util.Properties;

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
import org.kie.remote.command.FireUntilHaltCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.RemoteCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.kie.remote.CommonConfig.SKIP_LISTENER_AUTOSTART;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodAsLeaderTest {

    private final String TEST_KAFKA_LOGGER_TOPIC = "logs";
    private final String TEST_TOPIC = "test";
    private KafkaUtilTest kafkaServerTest;
    private Logger logger = LoggerFactory.getLogger(PodAsLeaderTest.class);
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
        try {
            Bootstrap.stopEngine();
        } catch (ConcurrentModificationException ex) {
        }
        kafkaServerTest.shutdownServer();
    }

    @Test
    public void processOneSentMessageAsLeaderTest() {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    config.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlConsumerProcessOneSentMessageAsLeaderTest"));

        Properties props = (Properties) Config.getProducerConfig( "InsertBactchStockTickets" ).clone();
        props.put( SKIP_LISTENER_AUTOSTART, true );

        kafkaServerTest.insertBatchStockTicketEvent(1, topicsConfig, RemoteKieSession.class, props);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(2, eventsRecords.count());
            Iterator<ConsumerRecord<String, byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String, byte[]> eventsRecord = eventsRecordIterator.next();
            assertEquals(eventsRecord.topic(), config.getEventsTopicName());
            RemoteCommand remoteCommand = deserialize(eventsRecord.value());

            assertEquals(eventsRecord.offset(), 0);
            assertNotNull(remoteCommand.getId());
            assertTrue(remoteCommand instanceof FireUntilHaltCommand);

            ConsumerRecord<String, byte[]> eventsRecordTwo = eventsRecordIterator.next();
            assertEquals(eventsRecordTwo.topic(), config.getEventsTopicName());
            remoteCommand = deserialize(eventsRecordTwo.value());

            assertEquals(eventsRecordTwo.offset(), 1);
            assertNotNull(remoteCommand.getId());
            assertTrue(remoteCommand instanceof InsertCommand);

            //CONTROL TOPIC
            List<ControlMessage> messages = new ArrayList<>();
            while(messages.size()<2) {
                ConsumerRecords controlRecords = controlConsumer.poll(2000);
                Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
                ConsumerRecord<String, byte[]> controlRecord = controlRecordIterator.next();
                ControlMessage controlMessage = deserialize(controlRecord.value());
                messages.add(controlMessage);
            }

            assertEquals(2, messages.size());
            Iterator<ControlMessage> messagesIter = messages.iterator();

            ControlMessage fireUntilHalt = messagesIter.next();
            ControlMessage insert = messagesIter.next();

            assertEquals(fireUntilHalt.getKey(),eventsRecord.key());
            assertTrue(fireUntilHalt.getSideEffects().isEmpty());

            assertEquals(insert.getKey(),eventsRecordTwo.key());
            assertTrue(!insert.getSideEffects().isEmpty());
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }

    @Test
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
                ConsumerRecords controlRecords = controlConsumer.poll(2000);
                Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
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
            logger.error(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            snapshotConsumer.close();
        }
    }
}
