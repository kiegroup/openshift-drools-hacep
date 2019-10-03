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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.message.ControlMessage;

public interface DefaultKafkaConsumerLifecycleProxy<T> {

    void internalRestartConsumer();

    void internalAskAndProcessSnapshotOnDemand();

    void internalAssign();

    void internalAssignAsALeader();

    void internalAssignReplica();

    void internalAssignConsumer(Consumer<String, T> kafkaConsumer, String topic);

    void internalUpdateOnRunningConsumer(State state);

    void internalRestart(State state);

    void internalEnableConsumeAndStartLoop(State state);

    void internalSetLastProcessedKey();

    void internalSettingsOnAEmptyControlTopic(ControlMessage lastWrapper);

    void internalAssignAndStartConsume();

    void internalConsume(int millisTimeout);

    void internalDefaultProcessAsLeader(int millisTimeout);

    void internalProcessLeader(ConsumerRecord<String, T> record);

    void internalConsumeEventsFromBufferAsALeader();

    void internalHandleSnapshotBetweenIteration(ConsumerRecord<String, T> record);

    void internalDefaultProcessAsAReplica(int millisTimeout);

    void internalConsumeEventsFromBufferAsAReplica();

    void internalConsumeControlFromBufferAsAReplica();

    void internalProcessEventsAsAReplica(ConsumerRecord<String, T> record);

    void internalProcessControlAsAReplica(ConsumerRecord<String, T> record);

    void internalSaveOffset(ConsumerRecord<String, T> record, Consumer<String, T> kafkaConsumer) ;

    void internalStartConsume();

    void internalStopConsume();

    void internalPollControl();

    void internalPollEvents();

    void internalPollNothing();
}
