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
package org.kie.remote.impl.consumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.remote.CommonConfig;
import org.kie.remote.TopicsConfig;

import org.kie.remote.message.FactCountMessage;
import org.kie.remote.message.ListKieSessionObjectMessage;
import org.kie.remote.message.VisitableMessage;
import org.kie.remote.message.VisitorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;

public class ListenerThread implements Runnable,
                                       VisitorMessage {

    private static Logger logger = LoggerFactory.getLogger(ListenerThread.class);
    private Properties configuration;
    private TopicsConfig topicsConfig;
    private Map<String, CompletableFuture<Object>> requestsStore;
    private KafkaConsumer consumer;
    private List<VisitableMessage> buffer;

    public ListenerThread(Properties configuration, TopicsConfig config, Map<String, CompletableFuture<Object>> requestsStore){
        this.configuration = configuration;
        this.topicsConfig = config;
        this.requestsStore = requestsStore;
        this.buffer = new ArrayList<>();
        prepareConsumer();
    }

    private void prepareConsumer() {
        consumer = new KafkaConsumer(configuration);
        List<PartitionInfo> infos = consumer.partitionsFor(topicsConfig.getKieSessionInfosTopicName());
        List<TopicPartition> partitions = new ArrayList<>();
        if (infos != null) {
            for (PartitionInfo partition : infos) {
                partitions.add(new TopicPartition(topicsConfig.getKieSessionInfosTopicName(), partition.partition()));
            }
        }
        consumer.assign(partitions);

        Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
        Long lastOffset = 0l;
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            lastOffset = entry.getValue();
        }
        if(lastOffset == 0){
            lastOffset = 1l;// this is to start the seek with offset -1 on empty topic
        }
        Set<TopicPartition> assignments = consumer.assignment();
        for (TopicPartition part : assignments) {
            consumer.seek(part, lastOffset - 1);
        }
    }

    @Override
    public void run() {
        try {
            while(true){
                ConsumerRecords records = consumer.poll(Duration.of(CommonConfig.DEFAULT_POLL_TIMEOUT_MS, ChronoUnit.MILLIS));
                for (Object item : records) {
                    ConsumerRecord<String, byte[]> record = (ConsumerRecord<String, byte[]>) item;
                    Object msg = deserialize(record.value());
                    VisitableMessage visitable = (VisitableMessage) msg;
                    visitable.accept(this);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            consumer.close();
        }
    }

    @Override
    public void visit(FactCountMessage msg, String key) {
        CompletableFuture<Object> completableFuture = requestsStore.get(msg.getKey());
        if(completableFuture!= null) {
            completableFuture.complete(msg.getFactCount());
            if(logger.isDebugEnabled()){
                logger.debug("completed msg with key {}",msg.getKey());
            }
        }
    }

    @Override
    public void visit(ListKieSessionObjectMessage msg, String key) {
        CompletableFuture<Object> completableFuture = requestsStore.get(msg.getKey());
        if(completableFuture!= null) {
            completableFuture.complete(msg.getObjects());
            if(logger.isDebugEnabled()){
                logger.debug("completed msg with key {}",msg.getKey());
            }
        }
    }
}
