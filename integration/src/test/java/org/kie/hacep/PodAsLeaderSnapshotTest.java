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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.SnapshotMessage;
import org.kie.remote.RemoteKieSession;

import static org.junit.Assert.*;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodAsLeaderSnapshotTest extends KafkaFullTopicsTests{

    @Test(timeout = 30000)
    public void processMessagesAsLeaderAndCreateSnapshotTest() {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   envConfig.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsProcessMessagesAsLeaderAndCreateSnapshotTest"));
        KafkaConsumer snapshotConsumer = kafkaServerTest.getConsumer("",
                                                                     envConfig.getSnapshotTopicName(),
                                                                     Config.getSnapshotConsumerConfig());
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    envConfig.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlProcessMessagesAsLeaderAndCreateSnapshotTest"));

        kafkaServerTest.insertBatchStockTicketEvent(10,
                                                    topicsConfig,
                                                    RemoteKieSession.class);
        try {

            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(5000);
            assertEquals(11, eventsRecords.count()); //1 fireUntilHalt + 10 stock ticket

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

            ConsumerRecords controlRecords = controlConsumer.poll(5000);
            assertEquals(11, controlRecords.count()); //1 fireUntilHalt + 10 stock ticket
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            snapshotConsumer.close();
        }
    }
}
