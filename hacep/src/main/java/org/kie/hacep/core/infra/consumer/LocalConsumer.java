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

import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.infra.election.State;
import org.kie.remote.DroolsExecutor;
import org.kie.remote.command.RemoteCommand;
import org.kie.remote.util.LocalMessageSystem;

public class LocalConsumer implements EventConsumer {

    private final LocalMessageSystem queue = LocalMessageSystem.get();

    private final EnvConfig config;

    private ConsumerHandler consumerHandler;

    private State currentState;

    public LocalConsumer( EnvConfig config ) {
        this.config = config;
    }

    @Override
    public void initConsumer( ConsumerHandler consumerHandler ) {
        this.consumerHandler = consumerHandler;
    }

    @Override
    public void poll( int durationMillis ) {
        String topic = config.getEventsTopicName();
        while (true) {
            RemoteCommand command = ( RemoteCommand ) queue.poll( topic, durationMillis );
            if (command != null) {
                consumerHandler.process( command, currentState );
            } else {
                break;
            }
        }
    }

    @Override
    public void stop() { }

    @Override
    public void updateStatus( State state ) {
        this.currentState = state;
        if (state == State.REPLICA) {
            DroolsExecutor.setAsReplica();
        } else {
            DroolsExecutor.setAsLeader();
        }
    }
}
