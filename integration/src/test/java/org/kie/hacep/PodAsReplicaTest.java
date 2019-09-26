package org.kie.hacep;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.RemoteKieSession;
import org.kie.remote.command.FireUntilHaltCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.RemoteCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class PodAsReplicaTest extends KafkaFullTopicsTests {

    private Logger logger = LoggerFactory.getLogger(PodAsReplicaTest.class);

    @Test(timeout = 30000L)
    public void processOneSentMessageAsLeaderAndThenReplicaTest() {
        Bootstrap.startEngine(envConfig);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer(envConfig.getEventsTopicName(),
                                                                   Config.getConsumerConfig("eventsConsumerProcessOneSentMessageAsLeaderTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer(envConfig.getControlTopicName(),
                                                                    Config.getConsumerConfig("controlConsumerProcessOneSentMessageAsLeaderTest"));

        KafkaConsumer<byte[], java.lang.String> kafkaLogConsumer = kafkaServerTest.getStringConsumer(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    topicsConfig,
                                                    RemoteKieSession.class);

        try {
            //EVENTS TOPIC
            logger.warn("Checks on Events topic");
            ConsumerRecords eventsRecords = eventsConsumer.poll(Duration.ofSeconds(2));

            AtomicReference<ConsumerRecord<String, byte[]>> lastEvent = new AtomicReference<>();
            final AtomicInteger index = new AtomicInteger(0);
            final AtomicInteger attempts = new AtomicInteger(0);
            while (index.get() < 2) {
                eventsRecords.forEach(o -> {
                    ConsumerRecord<String, byte[]> eventsRecord = (ConsumerRecord<String, byte[]>) o;
                    assertNotNull(eventsRecord);
                    assertEquals(eventsRecord.topic(), envConfig.getEventsTopicName());
                    RemoteCommand remoteCommand = deserialize(eventsRecord.value());
                    assertEquals(eventsRecord.offset(), index.get());
                    assertNotNull(remoteCommand.getId());

                    if (index.get() == 0) {
                        assertTrue(remoteCommand instanceof FireUntilHaltCommand);
                    }

                    if (index.get() == 1) {
                        assertTrue(remoteCommand instanceof InsertCommand);
                        InsertCommand insertCommand = (InsertCommand) remoteCommand;
                        assertEquals(insertCommand.getEntryPoint(), "DEFAULT");
                        assertNotNull(insertCommand.getId());
                        assertNotNull(insertCommand.getFactHandle());
                        RemoteFactHandle remoteFactHandle = insertCommand.getFactHandle();
                        StockTickEvent eventsTicket = (StockTickEvent) remoteFactHandle.getObject();
                        assertEquals(eventsTicket.getCompany(), "RHT");
                    }

                    index.incrementAndGet();

                    if (index.get() > 2) {
                        throw new RuntimeException("Found " + index.get() + " messages, more than the 2 expected.");
                    }
                    lastEvent.set(eventsRecord);
                });
                logger.warn("Attempt number:{}", attempts.incrementAndGet());
                if(attempts.get() == 10){
                    throw new RuntimeException("No Events message available after "+attempts + "attempts.");
                }
            }

            //CONTROL TOPIC
            logger.warn("Checks on Control topic");
            ConsumerRecords controlRecords = waitForControlMessage(controlConsumer);

            Iterator<ConsumerRecord<String, byte[]>> controlRecordIterator = controlRecords.iterator();
            if (controlRecordIterator.hasNext()) {
                checkFireSideEffects(controlRecordIterator.next());

                if (controlRecords.count() == 2) {
                    checkInsertSideEffects(lastEvent.get(), controlRecordIterator.next());
                } else {
                    // wait for second control message
                    controlRecords = waitForControlMessage(controlConsumer);
                    checkInsertSideEffects(lastEvent.get(), (ConsumerRecord<String, byte[]>) controlRecords.iterator().next());
                }
            }

            //no more msg to consume as a leader
            eventsRecords = eventsConsumer.poll(Duration.ofSeconds(2));
            assertEquals(0, eventsRecords.count());
            controlRecords = controlConsumer.poll(Duration.ofSeconds(2));
            assertEquals(0, controlRecords.count());

            // SWITCH AS a REPLICA
            logger.warn("Switch as a replica");
            Bootstrap.getConsumerController().getCallback().updateStatus(State.REPLICA);
            ConsumerRecords<byte[], String> recordsLog = kafkaLogConsumer.poll(Duration.ofSeconds(5));
            Iterator<ConsumerRecord<byte[], String>> recordIterator = recordsLog.iterator();
            java.util.List<java.lang.String> kafkaLoggerMsgs = new ArrayList<>();

            while (recordIterator.hasNext()) {
                ConsumerRecord<byte[], String> record = recordIterator.next();
                assertNotNull(record);
                kafkaLoggerMsgs.add(record.value());
                if(envConfig.isUnderTest()){
                    logger.warn("msg:{}", record.value());
                }

            }
            String sideEffectOnLeader = null;
            String sideEffectOnReplica = null;
            for (String item : kafkaLoggerMsgs) {
                if (item.startsWith("sideEffectOn")) {
                    if (item.endsWith(":null")) {
                        fail("SideEffects null");
                    }
                    if (item.startsWith("sideEffectOnReplica:")) {
                        sideEffectOnReplica = item.substring(item.indexOf("["));
                    }
                    if (item.startsWith("sideEffectOnLeader:")) {
                        sideEffectOnLeader = item.substring(item.indexOf("["));
                    }
                }
            }
            assertNotNull(sideEffectOnLeader);
            assertNotNull(sideEffectOnReplica);
            assertEquals(sideEffectOnLeader, sideEffectOnReplica);
        } catch (Exception ex) {
            logger.error(ex.getMessage(),
                         ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
            kafkaLogConsumer.close();
        }
    }

    private ConsumerRecords waitForControlMessage(KafkaConsumer controlConsumer) {
        ConsumerRecords controlRecords = controlConsumer.poll(Duration.ofSeconds(1));
        int attempts = 0;
        while (controlRecords.count() == 0) {
            controlRecords = controlConsumer.poll(Duration.ofSeconds(1));
            attempts ++;
            if(attempts == 10){
                throw new RuntimeException("No control message available after "+attempts + "attempts in waitForControlMessage");
            }
        }
        return controlRecords;
    }

    private void checkFireSideEffects(ConsumerRecord<String, byte[]> controlRecord) {
        // FireUntilHalt command has no side effects
        assertEquals(controlRecord.topic(), envConfig.getControlTopicName());
        ControlMessage controlMessage = deserialize(controlRecord.value());
        assertEquals(controlRecord.offset(), 0);
        assertTrue(controlMessage.getSideEffects().isEmpty());
    }

    private void checkInsertSideEffects(ConsumerRecord<String, byte[]> eventsRecord, ConsumerRecord<String, byte[]> controlRecord) {
        assertEquals(controlRecord.topic(), envConfig.getControlTopicName());
        ControlMessage controlMessage = deserialize(controlRecord.value());
        assertEquals(controlRecord.offset(), 1);
        assertTrue(!controlMessage.getSideEffects().isEmpty());
        assertTrue(controlMessage.getSideEffects().size() == 1);
        //Same msg content on Events topic and control topics
        assertEquals(controlRecord.key(), eventsRecord.key());
    }
}
