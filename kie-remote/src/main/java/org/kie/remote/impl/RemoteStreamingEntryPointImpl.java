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
package org.kie.remote.impl;

import java.util.Map;

import org.kie.remote.RemoteStreamingEntryPoint;
import org.kie.remote.TopicsConfig;
import org.kie.remote.command.EventInsertCommand;
import org.kie.remote.impl.producer.Sender;

public class RemoteStreamingEntryPointImpl extends AbstractRemoteEntryPoint implements RemoteStreamingEntryPoint {

    protected final RemoteStatefulSessionImpl delegate;

    public RemoteStreamingEntryPointImpl( Sender sender, String entryPoint, TopicsConfig topicsConfig, Map requestsStore) {
        super(sender, entryPoint, topicsConfig, requestsStore);
        delegate = new RemoteStatefulSessionImpl( sender, requestsStore, topicsConfig );
    }

    public RemoteStreamingEntryPointImpl( Sender sender, String entryPoint, TopicsConfig topicsConfig, Map requestsStore, RemoteStatefulSessionImpl delegate) {
        super(sender, entryPoint, topicsConfig, requestsStore);
        this.delegate = delegate;
    }

    @Override
    public void insert(Object object) {
        EventInsertCommand command = new EventInsertCommand(object, entryPoint);
        sender.sendCommand(command, topicsConfig.getEventsTopicName());
    }
}