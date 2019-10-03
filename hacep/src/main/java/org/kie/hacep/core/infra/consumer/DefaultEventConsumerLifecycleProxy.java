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
import org.kie.hacep.Config;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.consumer.DroolsConsumerHandler;
import org.kie.hacep.core.infra.DefaultSessionSnapShooter;
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

public class DefaultEventConsumerLifecycleProxy<T> implements EventConsumerLifecycleProxy {

    private Logger logger = LoggerFactory.getLogger(DefaultKafkaConsumer.class);
    private Logger loggerForTest;
    EventConsumerStatus status;
    private SnapshotInfos snapshotInfos;
    private DefaultSessionSnapShooter snapShooter;
    private Consumer kafkaConsumer, kafkaSecondaryConsumer;
    protected List<ConsumerRecord<String,T>> eventsBuffer;
    protected List<ConsumerRecord<String,T>> controlBuffer;
    private DroolsConsumerHandler consumerHandler;
    private volatile String processingKey = "";
    private volatile long processingKeyOffset, lastProcessedControlOffset, lastProcessedEventOffset;
    private volatile boolean askedSnapshotOnDemand;
    private int iterationBetweenSnapshot;
    private AtomicInteger counter ;
    private volatile PolledTopic polledTopic = PolledTopic.CONTROL;
    private Printer printer;

    private EnvConfig config;

    public DefaultEventConsumerLifecycleProxy(DroolsConsumerHandler consumerHandler, EnvConfig config, DefaultSessionSnapShooter snapShooter){
        status = new EventConsumerStatus();
        this.consumerHandler = consumerHandler;
        this.config = config;
        this.snapShooter = snapShooter;
        if (this.config.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(this.config);
        }
        if(this.config.isSkipOnDemanSnapshot()){
            counter = new AtomicInteger(0);
        }
        this.printer = PrinterUtil.getPrinter(this.config);
        iterationBetweenSnapshot = this.config.getIterationBetweenSnapshot();
        this.printer = PrinterUtil.getPrinter(this.config);
        if (this.config.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(this.config);
        }
    }

    public EventConsumerStatus getStatus(){
        return status;
    }

    public void internalRestartConsumer() {
        if (logger.isInfoEnabled()) {
            logger.info("Restart Consumers");
        }
        snapshotInfos = snapShooter.deserialize();//is still useful ?
        kafkaConsumer = new KafkaConsumer<>(Config.getConsumerConfig("PrimaryConsumer"));
        internalAssign();
        if (status.getCurrentState().equals(State.REPLICA)) {
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
        } else {
            kafkaSecondaryConsumer = null;
        }
    }

    public void internalAskAndProcessSnapshotOnDemand() {
        askedSnapshotOnDemand = true;
        boolean completed = consumerHandler.initializeKieSessionFromSnapshotOnDemand(config);
        if (logger.isInfoEnabled()) {
            logger.info("askAndProcessSnapshotOnDemand:{}", completed);
        }
        if (!completed) {
            throw new RuntimeException("Can't obtain a snapshot on demand");
        }
    }

    public  void internalAssign() {
        if (status.getCurrentState().equals(State.LEADER)) {
            internalAssignAsALeader();
        } else {
            internalAssignReplica();
        }
    }

    public  void internalAssignAsALeader() {
        internalAssignConsumer(kafkaConsumer, config.getEventsTopicName());
    }

    public void internalAssignReplica() {
        internalAssignConsumer(kafkaConsumer, config.getEventsTopicName());
        internalAssignConsumer(kafkaSecondaryConsumer, config.getControlTopicName());
    }

    public  void internalUpdateOnRunningConsumer(State state) {
        logger.info("updateOnRunning COnsumer");
        if (state.equals(State.LEADER) ) {
            DroolsExecutor.setAsLeader();
            internalRestart(state);
        } else if (state.equals(State.REPLICA)) {
            DroolsExecutor.setAsReplica();
            internalRestart(state);
        }
    }

    public  void internalRestart(State state) {
        internalStopConsume();
        internalRestartConsumer();
        internalEnableConsumeAndStartLoop(state);
    }

    public  void internalEnableConsumeAndStartLoop(State state) {
        if (state.equals(State.LEADER)) {
            status.setCurrentState(State.LEADER);
            DroolsExecutor.setAsLeader();

        } else if (state.equals(State.REPLICA) ) {
            status.setCurrentState(State.REPLICA);
            kafkaSecondaryConsumer = new KafkaConsumer<>(Config.getConsumerConfig("SecondaryConsumer"));
            DroolsExecutor.setAsReplica();
        }
        internalSetLastProcessedKey();
        internalAssignAndStartConsume();
    }

    public  void internalSetLastProcessedKey() {
        ControlMessage lastControlMessage = ConsumerUtils.getLastEvent(config.getControlTopicName(), config.getPollTimeout());
        internalSettingsOnAEmptyControlTopic(lastControlMessage);
        processingKey = lastControlMessage.getId();
        processingKeyOffset = lastControlMessage.getOffset();
    }

    public  void internalSettingsOnAEmptyControlTopic(ControlMessage lastWrapper) {
        if (lastWrapper.getId() == null) {// completely empty or restart of ephemeral already used
            if (status.getCurrentState().equals(State.REPLICA)) {
                internalPollControl();
            }
        }
    }

    public  void internalAssignAndStartConsume() {
        internalAssign();
        internalStartConsume();
    }

    public  void internalConsume(int millisTimeout) {
        if (status.isStarted()) {
            if (status.getCurrentState().equals(State.LEADER)) {
                internalDefaultProcessAsLeader(millisTimeout);
            } else {
                internalDefaultProcessAsAReplica(millisTimeout);
            }
        }
    }

    public  void internalDefaultProcessAsLeader(int millisTimeout) {
        internalPollEvents();
        if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
            internalConsumeEventsFromBufferAsALeader();
        }
        ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(millisTimeout, ChronoUnit.MILLIS));
        if (!records.isEmpty()) {
            ConsumerRecord<String, T> first = records.iterator().next();
            eventsBuffer = records.records(new TopicPartition(first.topic(), first.partition()));
            internalConsumeEventsFromBufferAsALeader();
        } else {
            internalPollControl();
        }
    }

    public  void internalConsumeEventsFromBufferAsALeader() {
        if (config.isUnderTest()) {
            loggerForTest.warn("Pre consumeEventsFromBufferAsALeader eventsBufferSize:{}", eventsBuffer.size());
        }
        int index = 0;
        int end = eventsBuffer.size();
        for (ConsumerRecord<String, T> record : eventsBuffer) {
            internalProcessLeader(record);
            index++;
            if (end > index) {
                eventsBuffer = eventsBuffer.subList(index, end);
            }
            break;
        }
        if (config.isUnderTest()) {
            loggerForTest.warn("post consumeEventsFromBufferAsALeader eventsBufferSize:{}", eventsBuffer.size());
        }
        if (end == index) {
            eventsBuffer = null;
        }
    }

    public void internalDefaultProcessAsAReplica(int millisTimeout) {
        if (polledTopic.equals(PolledTopic.EVENTS)) {
            if (eventsBuffer != null && eventsBuffer.size() > 0) { // events previously readed and not processed
                internalConsumeEventsFromBufferAsAReplica();
            }
            ConsumerRecords<String, T> records = kafkaConsumer.poll(Duration.of(millisTimeout,
                                                                                ChronoUnit.MILLIS));
            if (!records.isEmpty()) {
                ConsumerRecord<String, T> first = records.iterator().next();
                eventsBuffer = records.records(new TopicPartition(first.topic(),
                                                                  first.partition()));
                internalConsumeEventsFromBufferAsAReplica();
            } else {
                internalPollControl();
            }
        }

        if (polledTopic.equals(PolledTopic.CONTROL)) {

            if (controlBuffer != null && controlBuffer.size() > 0) {
                internalConsumeControlFromBufferAsAReplica();
            }

            ConsumerRecords<String, T> records = kafkaSecondaryConsumer.poll(Duration.of(millisTimeout,
                                                                                         ChronoUnit.MILLIS));
            if (records.count() > 0) {
                ConsumerRecord<String, T> first = records.iterator().next();
                controlBuffer = records.records(new TopicPartition(first.topic(),
                                                                   first.partition()));
                internalConsumeControlFromBufferAsAReplica();
            }
        }
    }

        public  void internalConsumeEventsFromBufferAsAReplica() {
            if (config.isUnderTest()) {
                loggerForTest.warn("consumeEventsFromBufferAsAReplica eventsBufferSize:{}", eventsBuffer.size());
            }
            int index = 0;
            int end = eventsBuffer.size();
            for (ConsumerRecord<String, T> record : eventsBuffer) {
                internalProcessEventsAsAReplica(record);
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

        public  void internalConsumeControlFromBufferAsAReplica() {
            if (config.isUnderTest()) {
                loggerForTest.warn("consumeControlFromBufferAsAReplica:{}", controlBuffer.size());
            }
            int index = 0;
            int end = controlBuffer.size();
            for (ConsumerRecord<String, T> record : controlBuffer) {
                internalProcessControlAsAReplica(record);
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



        public void internalSaveOffset(ConsumerRecord record, Consumer kafkaConsumer) {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            map.put(new TopicPartition(record.topic(),
                                       record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
            kafkaConsumer.commitSync(map);
        }

        public void internalProcessControlAsAReplica(ConsumerRecord record) {

            if (record.offset() == processingKeyOffset + 1 || record.offset() == 0) {
                lastProcessedControlOffset = record.offset();
                processingKey = record.key().toString();
                processingKeyOffset = record.offset();
                ControlMessage wr = deserialize((byte[]) record.value());
                consumerHandler.processSideEffectsOnReplica(wr.getSideEffects());

                internalPollEvents();
                if (logger.isDebugEnabled()) {
                    logger.debug("change topic, switch to consume events");
                }
            }
            if (processingKey == null) { // empty topic
                processingKey = record.key().toString();
                processingKeyOffset = record.offset();
            }
            internalSaveOffset(record, kafkaSecondaryConsumer);
        }

        public void internalProcessEventsAsAReplica(ConsumerRecord record) {

            ItemToProcess item = ItemToProcess.getItemToProcess(record);
            if (record.key().equals(processingKey)) {
                lastProcessedEventOffset = record.offset();

                internalPollControl();

                if (logger.isDebugEnabled()) {
                    logger.debug("processEventsAsAReplica change topic, switch to consume control, still {} events in the eventsBuffer to consume and processing item:{}.", eventsBuffer.size(), item );
                }
                consumerHandler.process(item, status.getCurrentState());
                internalSaveOffset(record, kafkaConsumer);

            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("processEventsAsAReplica still {} events in the eventsBuffer to consume and processing item:{}.", eventsBuffer.size(), item );
                }
                consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
                internalSaveOffset(record, kafkaConsumer);
            }
        }

        public void internalHandleSnapshotBetweenIteration(ConsumerRecord record) {
            int iteration = counter.incrementAndGet();
            if (iteration == iterationBetweenSnapshot) {
                counter.set(0);
                consumerHandler.processWithSnapshot(ItemToProcess.getItemToProcess(record), status.getCurrentState());
            } else {
                consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
            }
        }

        public  void internalProcessLeader(ConsumerRecord record) {

            if (config.isSkipOnDemanSnapshot()) {
                internalHandleSnapshotBetweenIteration(record);
            } else {
                consumerHandler.process(ItemToProcess.getItemToProcess(record), status.getCurrentState());
            }
            processingKey = record.key().toString();// the new processed became the new processingKey
            internalSaveOffset(record, kafkaConsumer);

            if (logger.isInfoEnabled() || config.isUnderTest()) {
                printer.prettyPrinter("DefaulImprovedKafkaConsumer.processLeader record:{}", record, true);
            }
        }

    public void internalAssignConsumer(Consumer kafkaConsumer, String topic) {

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
            if(status.getCurrentState().equals(State.LEADER)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedEventOffset));
            }else if(status.getCurrentState().equals(State.REPLICA)){
                kafkaConsumer.assignment().forEach(topicPartition -> kafkaConsumer.seek(partitionCollection.iterator().next(),
                                                                                        lastProcessedControlOffset));
            }
        }
    }


        public  void internalStartConsume() {
            status.setStarted(true);
        }

        public  void internalStopConsume() {
            status.setStarted(false);
        }

        public  void internalPollControl(){
            if(!polledTopic.equals(PolledTopic.CONTROL)) {
                polledTopic = PolledTopic.CONTROL;
            }
        }

        public  void internalPollEvents(){
            if(!polledTopic.equals(PolledTopic.EVENTS)) {
                polledTopic = PolledTopic.EVENTS;
            }
        }

        public  void internalPollNothing(){
            polledTopic = PolledTopic.NONE;
        }
}
