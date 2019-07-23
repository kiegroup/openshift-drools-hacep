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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.kie.remote.RemoteEntryPoint;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.TopicsConfig;
import org.kie.remote.command.DeleteCommand;
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.UpdateCommand;
import org.kie.remote.impl.RemoteFactHandleImpl;
import org.kie.remote.impl.consumer.Listener;

public class RemoteEntryPointImpl implements RemoteEntryPoint {

    protected final Sender sender;
    protected final Listener listener;
    private final String entryPoint;
    protected Map<String, CompletableFuture<Object>> requestsStore;
    private TopicsConfig topicsConfig;

    public RemoteEntryPointImpl(Sender sender, String entryPoint, TopicsConfig topicsConfig, Map requestsStore) {
        this.requestsStore = requestsStore;
        this.listener = new Listener(requestsStore);
        this.sender = sender;
        this.entryPoint = entryPoint;
        this.topicsConfig = topicsConfig;

    }

    public void listen(){
        listener.listen();
    }

    public void stop(){
        listener.stopConsumeEvents();
        requestsStore.clear();
    }

    @Override
    public String getEntryPointId() {
        return entryPoint;
    }

    @Override
    public RemoteFactHandle insert(Object obj) {
        RemoteFactHandle factHandle = new RemoteFactHandleImpl( obj );
        InsertCommand command = new InsertCommand(factHandle, entryPoint );
        sender.sendCommand(command, topicsConfig.getEventsTopicName());
        return factHandle;
    }

    @Override
    public void delete( RemoteFactHandle handle ) {
        DeleteCommand command = new DeleteCommand(handle, entryPoint );
        sender.sendCommand(command, topicsConfig.getEventsTopicName());
    }

    @Override
    public void update( RemoteFactHandle handle, Object object ) {
        UpdateCommand command = new UpdateCommand(handle, object, entryPoint);
        sender.sendCommand(command, topicsConfig.getEventsTopicName());
    }

    @Override
    public CompletableFuture getObjects() {
        throw new UnsupportedOperationException( "org.kie.hacep.producer.RemoteKieSessionImpl.getObjects -> TODO" );
    }

    @Override
    public CompletableFuture getObjects(Predicate filter) {
        throw new UnsupportedOperationException( "org.kie.hacep.producer.RemoteKieSessionImpl.getObjects -> TODO" );
    }

    @Override
    public CompletableFuture<Long> getFactCount() {
        CompletableFuture callback = new CompletableFuture<>();
        FactCountCommand command = new FactCountCommand(entryPoint);
        requestsStore.put(command.getId(), callback);
        sender.sendCommand(command, topicsConfig.getEventsTopicName());
        return callback;
    }

}