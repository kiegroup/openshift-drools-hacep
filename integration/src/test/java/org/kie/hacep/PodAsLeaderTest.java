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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.ControlMessage;
import org.kie.remote.RemoteKieSession;
import org.kie.remote.command.FireUntilHaltCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.RemoteCommand;

import static org.junit.Assert.*;
import static org.kie.remote.CommonConfig.SKIP_LISTENER_AUTOSTART;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodAsLeaderTest extends KafkaFullTopicsTests {

    @Test(timeout = 20000)
    public void processOneSentMessageAsLeaderTest() {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer(envConfig.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer(envConfig.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlConsumerProcessOneSentMessageAsLeaderTest"));

        Properties props = (Properties) Config.getProducerConfig("InsertBactchStockTickets").clone();
        props.put(SKIP_LISTENER_AUTOSTART, true);

        kafkaServerTest.insertBatchStockTicketEvent(1, topicsConfig, RemoteKieSession.class, props);
        try {
            //EVENTS TOPIC
            ConsumerRecords eventsRecords = eventsConsumer.poll(Duration.ofMillis(5000));
            assertEquals(2, eventsRecords.count());
            Iterator<ConsumerRecord<String,byte[]>> eventsRecordIterator = eventsRecords.iterator();
            ConsumerRecord<String,byte[]> eventsRecord = null;
            ConsumerRecord<String,byte[]> eventsRecordTwo = null;
            RemoteCommand remoteCommand ;

            if (eventsRecordIterator.hasNext()) {
                eventsRecord = eventsRecordIterator.next();
                assertEquals(eventsRecord.topic(), envConfig.getEventsTopicName());
                assertEquals(eventsRecord.offset(), 0);

                remoteCommand = deserialize(eventsRecord.value());
                assertNotNull(remoteCommand.getId());
                assertTrue(remoteCommand instanceof FireUntilHaltCommand);
            }

            if (eventsRecordIterator.hasNext()) {
                eventsRecordTwo = eventsRecordIterator.next();
                assertEquals(eventsRecordTwo.topic(), envConfig.getEventsTopicName());
                assertEquals(eventsRecordTwo.offset(), 1);

                remoteCommand = deserialize(eventsRecordTwo.value());
                assertNotNull(remoteCommand.getId());
                assertTrue(remoteCommand instanceof InsertCommand);
            }
            //CONTROL TOPIC
            List<ControlMessage> messages = new ArrayList<>();
            while (messages.size() < 2) {
                ConsumerRecords controlRecords = controlConsumer.poll(Duration.ofMillis(2000));
                Iterator<ConsumerRecord<String,byte[]>> controlRecordIterator = controlRecords.iterator();
                if(controlRecordIterator.hasNext()) {
                    ConsumerRecord<String,byte[]> controlRecord = controlRecordIterator.next();
                    ControlMessage controlMessage = deserialize(controlRecord.value());
                    messages.add(controlMessage);
                }
            }
            assertEquals(2, messages.size());

            ControlMessage fireUntilHalt = null;
            ControlMessage insert = null;
            Iterator<ControlMessage> messagesIter = messages.iterator();
            if(messagesIter.hasNext()) {
                fireUntilHalt = messagesIter.next();
            }
            if(messagesIter.hasNext()) {
                insert = messagesIter.next();
            }
            assertEquals(fireUntilHalt.getId(), eventsRecord.key());
            assertTrue(fireUntilHalt.getSideEffects().isEmpty());
            assertEquals(insert.getId(), eventsRecordTwo.key());
            assertTrue(!insert.getSideEffects().isEmpty());

        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
        }
    }
}
