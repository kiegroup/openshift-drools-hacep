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
package org.kie.hacep.core.infra.consumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.kie.hacep.Config;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.DeafultSessionSnapShooter;
import org.kie.hacep.core.infra.OffsetManager;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.ConsumerUtils;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.util.Printer;
import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;
/**
 * The default consumer relies on the Consumer thread and
 * is based on the loop around poll method.
 */
public class DefaultKafkaConsumer<T> implements EventConsumer {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsEvents = new HashMap<>();
    private Consumer<String, T> kafkaConsumer, kafkaSecondaryConsumer;
    private DroolsConsumerHandler consumerHandler;
    private volatile String processingKey = "";
    private volatile long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;
    private volatile boolean started = false;
    private volatile State currentState = State.REPLICA;
    private volatile PolledTopic polledTopic = PolledTopic.CONTROL;
    private int iterationBetweenSnapshot;
    private List<ConsumerRecord<String, T>> eventsBuffer;
    private List<ConsumerRecord<String, T>> controlBuffer;
    private AtomicInteger counter ;
    private SnapshotInfos snapshotInfos;
    private DeafultSessionSnapShooter snapShooter;
    private Printer printer;
    private EnvConfig config;
    private Logger loggerForTest;
    private volatile boolean askedSnapshotOnDemand;

    public DefaultKafkaConsumer(EnvConfig config) {
        this.config = config;
        if(this.config.isSkipOnDemanSnapshot()){
            counter = new AtomicInteger(0);
        }
        iterationBetweenSnapshot = this.config.getIterationBetweenSnapshot();
        this.printer = PrinterUtil.getPrinter(this.config);
        if (this.config.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(this.config);
        }
    }

    public void initConsumer(ConsumerHandler consumerHandler) {
        this.consumerHandler = (DroolsConsumerHandler) consumerHandler;
        this.snapShooter = this.consumerHandler.getSnapshooter();
        this.kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        if (currentState.equals(State.REPLICA)) {
            this.kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        }
    }

    protected void restartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        assign();
        if (currentState.equals(State.REPLICA)) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        } else {
            kafkaSecondaryConsumer = null;
        }
    }

    @Override
    public void stop() {
        stopConsume();
        pollNothing();
        kafkaConsumer.wakeup();
        if (kafkaSecondaryConsumer != null) {
            kafkaSecondaryConsumer.wakeup();
        }
        consumerHandler.stop();
    }

    @Override
    public void updateStatus(State state) {
        if(currentState == null ||  !state.equals(currentState)){
            currentState = state;
        }
        if (started) {
            updateOnRunningConsumer(state);
        } else {
            if (state.equals(State.REPLICA)) {
                //ask and wait a snapshot before start
                if (!config.isSkipOnDemanSnapshot() && !askedSnapshotOnDemand) {
                    if (logger.isInfoEnabled()) {
                        logger.info("askAndProcessSnapshotOnDemand:");
                    }
                    askAndProcessSnapshotOnDemand();
                }
            }
            //State.BECOMING_LEADER won't start the pod
            if (state.equals(State.LEADER) || state.equals(State.REPLICA)) {
                if (logger.isInfoEnabled()) {
                    logger.info("enableConsumeAndStartLoop:{}", state);
                }
                enableConsumeAndStartLoop(state);
            }
        }
    }

    protected void askAndProcessSnapshotOnDemand() {
        askedSnapshotOnDemand = true;
        boolean completed = consumerHandler.initializeKieSessionFromSnapshotOnDemand(config);
        if (logger.isInfoEnabled()) {
            logger.info("askAndProcessSnapshotOnDemand:{}", completed);
        }
        if (!completed) {
            throw new RuntimeException("Can't obtain a snapshot on demand");
        }
    }

    protected void assign() {
        if (currentState.equals(State.LEADER)) {
            assignAsALeader();
        } else {
            assignNotLeader();
        }
    }

    protected void assignAsALeader() {
        assignConsumer(kafkaConsumer, config.getEventsTopicName());
    }

    protected void assignNotLeader() {
        assignConsumer(kafkaConsumer, config.getEventsTopicName());
        assignConsumer(kafkaSecondaryConsumer, config.getControlTopicName());
    }



    protected void assignConsumer(Consumer<String, T> kafkaConsumer, String topic) {

        List<PartitionInfo> partitionsInfo = kafkaConsumer.partitionsFor(topic);
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                partitionCollection.add(new TopicPartition(partition.topic(), partition.partition()));
            }

            if (!partitionCollection.isEmpty()) {
                kafkaConsumer.assign(partitionCollection);
            }
        }

        if (snapshotInfos != null) {
            if (partitionCollection.size() > 1) {
                throw new RuntimeException("The system must run with only one partition per topic");
            }
            kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                    snapshotInfos.getOffsetDuringSnapshot()));
        } else {
            if(currentState.equals(State.LEADER)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedEventOffset));
            }else if(currentState.equals(State.REPLICA)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedControlOffset));
            }
        }
    }

    public void poll(int durationMillis) {

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Starting exit...\n");
            kafkaConsumer.wakeup();
            if (kafkaSecondaryConsumer != null) {
                kafkaSecondaryConsumer.wakeup();
            }
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(),
                             e);
            }
        }));

        if (kafkaConsumer == null) {
            throw new IllegalStateException("Can't poll, kafkaConsumer not subscribed or null!");
        }

        if (kafkaSecondaryConsumer == null) {
            throw new IllegalStateException("Can't poll, kafkaConsumer not subscribed or null!");
        }

        try {
            while (true) {
                consume(durationMillis);
            }
        } catch (WakeupException e) {
            //nothind to do
        } finally {
            try {
                kafkaConsumer.commitSync();
                kafkaSecondaryConsumer.commitSync();
                if (logger.isDebugEnabled()) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsEvents.entrySet()) {
                        logger.debug("Consumer partition %s - lastOffset %s\n",
                                     entry.getKey().partition(),
                                     entry.getValue().offset());
                    }
                }
                OffsetManager.store(offsetsEvents);
            } catch (WakeupException e) {
                //nothing to do
            } finally {
                logger.info("Closing kafkaConsumer on the loop");
                kafkaConsumer.close();
                kafkaSecondaryConsumer.close();
            }
        }
    }

    protected void updateOnRunningConsumer(State state) {
        if (state.equals(State.LEADER) ) {
            DroolsExecutor.setAsLeader();
            restart(state);
        } else if (state.equals(State.REPLICA)) {
            DroolsExecutor.setAsReplica();
            restart(state);
        }
    }

    protected void restart(State state) {
        stopConsume();
        restartConsumer();
        enableConsumeAndStartLoop(state);
    }

    protected void enableConsumeAndStartLoop(State state) {
        if (logger.isInfoEnabled()) {
            logger.info("enableConsumeAndStartLoop:state{} currentState:{}", state, currentState);
        }
        if (state.equals(State.LEADER)) {
            currentState = State.LEADER;
            DroolsExecutor.setAsLeader();

        } else if (state.equals(State.REPLICA) ) {
            currentState = State.REPLICA;
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
            DroolsExecutor.setAsReplica();
        }
        setLastProcessedKey();
        assignAndStartConsume();
    }

    protected void setLastProcessedKey() {
        ControlMessage lastControlMessage = ConsumerUtils.getLastEvent(config.getControlTopicName(), config.getPollTimeout());
        settingsOnAEmptyControlTopic(lastControlMessage);
        processingKey = lastControlMessage.getId();
        processingKeyOffset = lastControlMessage.getOffset();
    }

    protected void settingsOnAEmptyControlTopic(ControlMessage lastWrapper) {
        if (lastWrapper.getId() == null) {// completely empty or restart of ephemeral already used
            if (currentState.equals(State.REPLICA)) {
                pollControl();
            }
        }
    }

    protected void assignAndStartConsume() {
        assign();
        startConsume();
    }

    protected void consume(int millisTimeout) {
        if (started) {
            if (currentState.equals(State.LEADER)) {
                defaultProcessAsLeader(millisTimeout);
            } else {
                defaultProcessAsAReplica(millisTimeout);
            }
        }
    }

    protected void defaultProcessAsLeader(int millisTimeout) {
        pollEvents();
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(millisTimeout, ChronoUnit.MILLIS));
        if(!records.isEmpty()){
            logger.info("records:{}", records.count());
            for (ConsumerRecord<String, T> record : records) {
                processLeader(record);
            }
        }
    }

    protected void processLeader(ConsumerRecord<String, T> record) {
        if (currentState.equals(State.LEADER)) {
            if (config.isSkipOnDemanSnapshot()) {
                handleSnapshotBetweenIteration(record);
            } else {
                consumerHandler.process(ItemToProcess.getItemToProcess(record), currentState);
            }
            processingKey = record.key();// the new processed became the new processingKey
            saveOffset(record, kafkaConsumer);
        }

        if (logger.isInfoEnabled() || config.isUnderTest()) {
            printer.prettyPrinter("DefaulImprovedKafkaConsumer.processLeader record:{}", record, true);
        }
    }

    protected void handleSnapshotBetweenIteration(ConsumerRecord<String, T> record) {
        int iteration = counter.incrementAndGet();
        if (iteration == iterationBetweenSnapshot) {
            counter.set(0);
            consumerHandler.processWithSnapshot(ItemToProcess.getItemToProcess(record), currentState);
        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), currentState);
        }
    }

    protected void defaultProcessAsAReplica(int millisTimeout) {
        if (polledTopic.equals(PolledTopic.EVENTS)) {
            if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
                consumeEventsFromBufferAsAReplica();
            }
            ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(millisTimeout, ChronoUnit.MILLIS));
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
                consumeEventsFromBufferAsAReplica();
            } else {
                pollControl();
            }
        }

        if (polledTopic.equals(PolledTopic.CONTROL)) {

            if (controlBuffer != null && controlBuffer.size() > 0) {
                consumeControlFromBufferAsAReplica();
            }

            ConsumerRecords<String, T> records = kafkaSecondaryConsumer.poll(Duration.of(millisTimeout, ChronoUnit.MILLIS));
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                controlBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
                consumeControlFromBufferAsAReplica();
            }
        }
    }

    protected void consumeEventsFromBufferAsAReplica() {
        int index = 0;
        int end = eventsBuffer.size();
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            processEventsAsAReplica(record);
            index++;
            if (polledTopic.equals(PolledTopic.CONTROL)) {
                if (end > index) {
                    eventsBuffer = eventsBuffer.subList(index, end);
                }
                break;
            }
        }
        if (end == index) {
            eventsBuffer = null;
        }
    }

    protected void consumeControlFromBufferAsAReplica() {
        if (config.isUnderTest()) {
            loggerForTest.warn("consumeControlFromBufferAsAReplica:{}", controlBuffer.size());
        }
        int index = 0;
        int end = controlBuffer.size();
        for (ConsumerRecord<String, T> record : controlBuffer) {
            processControlAsAReplica(record);
            index++;
            if (polledTopic.equals(PolledTopic.EVENTS)) {
                if (end > index) {
                    controlBuffer = controlBuffer.subList(index, end);
                }
                break;
            }
        }
        if (end == index) {
            controlBuffer = null;
        }
    }

    protected void processEventsAsAReplica(ConsumerRecord<String, T> record) {
        if (config.isUnderTest()) {
            loggerForTest.warn("DefaulKafkaConsumer.processEventsAsAReplica record:{}", record);
        }

        if (record.key().equals(processingKey)) {
            lastProcessedEventOffset = record.offset();
            if (logger.isInfoEnabled()) {
                logger.info("change topic, switch to consume control");
            }

            pollControl();
            consumerHandler.process(ItemToProcess.getItemToProcess(record), currentState);
            saveOffset(record, kafkaConsumer);

        } else {
            consumerHandler.process(ItemToProcess.getItemToProcess(record), currentState);
            saveOffset(record, kafkaConsumer);
        }
    }

    protected void processControlAsAReplica(ConsumerRecord<String, T> record) {
        if (config.isUnderTest()) {
            loggerForTest.warn("DefaulKafkaConsumer.processControlAsAReplica record:{}", record);
        }
        if (record.offset() == processingKeyOffset + 1 || record.offset() == 0) {
            lastProcessedControlOffset = record.offset();
            processingKey = record.key();
            processingKeyOffset = record.offset();
            ControlMessage wr = deserialize((byte[]) record.value());
            consumerHandler.processSideEffectsOnReplica(wr.getSideEffects());

            pollEvents();
            if (logger.isDebugEnabled()) {
                logger.debug("change topic, switch to consume events");
            }
        }
        if (processingKey == null) { // empty topic
            processingKey = record.key();
            processingKeyOffset = record.offset();
        }
        saveOffset(record,
                   kafkaSecondaryConsumer);
    }

    protected void saveOffset(ConsumerRecord<String, T> record,
                            Consumer<String, T> kafkaConsumer) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(),
                                   record.partition()),
                new OffsetAndMetadata(record.offset() + 1));
        kafkaConsumer.commitSync(map);
    }

    protected void startConsume() {
        started = true;
    }

    protected void stopConsume() {
        started = false;
    }

    protected void pollControl(){
        if(!polledTopic.equals(PolledTopic.CONTROL)) {
            polledTopic = PolledTopic.CONTROL;
        }
    }

    protected void pollEvents(){
        if(!polledTopic.equals(PolledTopic.EVENTS)) {
            polledTopic = PolledTopic.EVENTS;
        }
    }

    protected void pollNothing(){
        polledTopic = PolledTopic.NONE;
    }
}