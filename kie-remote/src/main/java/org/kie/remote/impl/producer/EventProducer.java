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
package org.kie.remote.impl.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.serialize;

public class EventProducer<T> implements Producer {

    private Logger logger = LoggerFactory.getLogger(EventProducer.class);
    protected org.apache.kafka.clients.producer.Producer<String, T> producer;

    public void start(Properties properties) {
        producer = new KafkaProducer(properties);
    }

    public void stop() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    public void produceFireAndForget(String topicName, String key, Object object) {
        producer.send(getFreshProducerRecord(topicName, key, object));
    }


    public long produceSync(String topicName, String key, Object object) {
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = producer.send(getFreshProducerRecord(topicName, key, object)).get();
        } catch (InterruptedException e) {
            logger.error("Error in produceSync!", e);
        } catch (ExecutionException e) {
            logger.error("Error in produceSync!", e);
        }
        return  recordMetadata.offset();
    }


    public void produceAsync(String topicName, String key, Object object, Callback callback) {
        producer.send(getFreshProducerRecord(topicName, key, object), callback);
    }


    private ProducerRecord<String, T> getFreshProducerRecord(String topicName, String key, Object object){
        return new ProducerRecord<>(topicName, key, (T) serialize(object));
    }
}
