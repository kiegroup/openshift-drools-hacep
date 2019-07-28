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

import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class KafkaTest {

    private final String TEST_KAFKA_LOGGER_TOPIC = "logs";
    private final String TEST_TOPIC = "test";
    private KafkaUtilTest kafkaServerTest;
    private Logger kafkaLogger = LoggerFactory.getLogger("org.hacep");

    @Before
    public void setUp() throws Exception {
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopics(TEST_KAFKA_LOGGER_TOPIC,
                                     TEST_TOPIC);
    }

    @After
    public void tearDown() {
        kafkaServerTest.shutdownServer();
    }

    @Test
    public void basicTest() {
        KafkaProducer<String, byte[]> producer = kafkaServerTest.getByteArrayProducer();
        KafkaConsumer<String, byte[]> consumer = kafkaServerTest.getByteArrayConsumer(TEST_TOPIC);

        ProducerRecord<String, byte[]> data = new ProducerRecord<>(TEST_TOPIC,
                                                                 "42",
                                                                 Base64.encodeBase64("test-message".getBytes(Charset.forName("UTF-8"))));
        kafkaServerTest.sendSingleMsg(producer, data);

        ConsumerRecords<String, byte[]> records = consumer.poll(10000);
        assertEquals(1, records.count());
        Iterator<ConsumerRecord<String, byte[]>> recordIterator = records.iterator();
        ConsumerRecord<String, byte[]> record = recordIterator.next();

        assertEquals("42", record.key());
        assertEquals("test-message", new String(Base64.decodeBase64(record.value())));
    }

    @Test
    public void testKafkaLoggerWithStringTest() {
        KafkaConsumer<String, String> consumerKafkaLogger = kafkaServerTest.getStringConsumer(TEST_KAFKA_LOGGER_TOPIC);
        kafkaLogger.warn("test-message");
        ConsumerRecords<String, String> records = consumerKafkaLogger.poll(10000);
        assertEquals(1, records.count());
        Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
        ConsumerRecord<String, String> record = recordIterator.next();
        assertNotNull(record);
        assertEquals(record.topic(), TEST_KAFKA_LOGGER_TOPIC);
        assertEquals(record.value(), "test-message");
    }
}
