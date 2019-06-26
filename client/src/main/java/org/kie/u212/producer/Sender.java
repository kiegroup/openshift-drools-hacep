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
package org.kie.u212.producer;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.kie.remote.RemoteCommand;
import org.kie.u212.ClientUtils;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.model.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sender {

  private static Logger logger = LoggerFactory.getLogger( RemoteKieSessionImpl.class);
  private EventProducer producer;
  private Properties configuration;

  public Sender(Properties configuration){
    producer = new EventProducer();
    if(configuration != null && !configuration.isEmpty()) {
      this.configuration = configuration;
    }
  }

  public void start() {
    producer.start(configuration != null ? configuration : ClientUtils.getConfiguration(ClientUtils.PRODUCER_CONF));
  }

  public void stop() {
    producer.stop();
  }

  public void sendCommand( RemoteCommand command, String topicName) {
    producer.produceSync(topicName, command.getId(),command);
  }

  // TODO is this useful? I think we should remove it
  public void insertAsync(Object obj, String topicName,
                          Callback callback) {
    ControlMessage event = wrapObject(obj);
    producer.produceAsync(topicName, event.getKey(), event, callback);
  }

  public void insertFireAndForget(Object obj, String topicName) {
    ControlMessage event = ( ControlMessage ) obj;
    producer.produceFireAndForget(topicName, event.getKey(), event);
  }

  private ControlMessage wrapObject( Object obj){
    ControlMessage event = new ControlMessage(UUID.randomUUID().toString(), 0l);
    return  event;
  }
}
