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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.runtime.KieSession;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.consumer.DefaultKafkaConsumer;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.ControlMessage;
import org.kie.remote.CommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.kie.remote.util.SerializationUtil.deserialize;

public class KieSessionBetweenUpdateStatusTest {

    private KafkaUtilTest kafkaServerTest;
    private EnvConfig config;
    private Logger logger = LoggerFactory.getLogger(SnapshotOnDemandAsAReplicaTest.class);

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
                underTest(true);
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
        kafkaServerTest.tearDown();
    }

    @Test(timeout = 20000L)
    public void kieSessionBetweenUpdateStatusTest() {
        Bootstrap.startEngine(config);
        KafkaConsumer eventsConsumer = kafkaServerTest.getConsumer("",
                                                                   config.getEventsTopicName(),
                                                                   Config.getConsumerConfig("SnapshotOnDemandAsAReplicaTest.createSnapshotOnDemandTest"));
        KafkaConsumer controlConsumer = kafkaServerTest.getConsumer("",
                                                                    config.getControlTopicName(),
                                                                    Config.getConsumerConfig("SnapshotOnDemandAsAReplicaTest.createSnapshotOnDemandTest"));

        KafkaConsumer snapshotConsumer = kafkaServerTest.getConsumer("",
                                                                     config.getSnapshotTopicName(),
                                                                     Config.getConsumerConfig("SnapshotOnDemandAsAReplicaTest.createSnapshotOnDemandTest"));
        try {
            AtomicReference<KieSession> sessionTestUpdater = new AtomicReference<>();
            Thread updater = new Thread(()->{
                try {
                    Thread.sleep(3000);
                    Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
                    KieSession kx = ((DefaultKafkaConsumer) Bootstrap.getConsumerController().getConsumer()).getConsumerHandler().getKieSessionContext().getKieSession();
                    sessionTestUpdater.set(kx);
                }catch (Exception e){ }
            });
            updater.start();
            Bootstrap.getConsumerController().getCallback().updateStatus(State.REPLICA);
            Thread.sleep(3000);
            KieSession sessionTest =((DefaultKafkaConsumer)Bootstrap.getConsumerController().getConsumer()).getConsumerHandler().getKieSessionContext().getKieSession();
            assertEquals(sessionTest.getId(), sessionTestUpdater.get().getIdentifier());

            ConsumerRecords eventsRecords = eventsConsumer.poll(1000);
            assertEquals(1, eventsRecords.count());

            ConsumerRecords controlRecords;
            List<ControlMessage> messages = new ArrayList<>();
            while (messages.size() < 1) {
                controlRecords = controlConsumer.poll(5000);
                Iterator<ConsumerRecord<String, byte[]>> controlIterator = controlRecords.iterator();
                if (controlIterator.hasNext()) {
                    ConsumerRecord<String, byte[]> controlRecord = controlIterator.next();
                    ControlMessage snapshotMessage = deserialize(controlRecord.value());
                    messages.add(snapshotMessage);
                }
            }

            assertEquals(1, messages.size());

            ConsumerRecords snapshotRecords = snapshotConsumer.poll(1000);
            assertEquals(1, snapshotRecords.count());

        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            eventsConsumer.close();
            controlConsumer.close();
            snapshotConsumer.close();
        }
    }
}
