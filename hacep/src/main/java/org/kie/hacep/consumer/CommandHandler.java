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
package org.kie.hacep.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.kie.api.runtime.rule.FactHandle;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.KieSessionContext;
import org.kie.hacep.message.FactCountMessageImpl;
import org.kie.hacep.message.ListKieSessionObjectMessageImpl;
import org.kie.remote.RemoteFactHandle;
import org.kie.remote.command.DeleteCommand;
import org.kie.remote.command.EventInsertCommand;
import org.kie.remote.command.FactCountCommand;
import org.kie.remote.command.FireAllRulesCommand;
import org.kie.remote.command.FireUntilHaltCommand;
import org.kie.remote.command.HaltCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.ListObjectsCommand;
import org.kie.remote.command.ListObjectsCommandClassType;
import org.kie.remote.command.ListObjectsCommandNamedQuery;
import org.kie.remote.command.UpdateCommand;
import org.kie.remote.command.VisitorCommand;
import org.kie.remote.impl.producer.Producer;

public class CommandHandler implements VisitorCommand {

    private KieSessionContext kieSessionContext;
    private EnvConfig config;
    private Producer producer;

    private volatile boolean firingUntilHalt;

    public CommandHandler(KieSessionContext kieSessionContext,
                          EnvConfig config,
                          Producer producer) {
        this.kieSessionContext = kieSessionContext;
        this.config = config;
        this.producer = producer;
    }

    @Override
    public void visit(FireAllRulesCommand command) {
        int fires = kieSessionContext.getKieSession().fireAllRules();
        producer.produceSync(config.getKieSessionInfosTopicName(), command.getId(), fires);
    }

    @Override
    public void visit(FireUntilHaltCommand command) {
        firingUntilHalt = true;
    }

    @Override
    public void visit(HaltCommand command) {
        firingUntilHalt = false;
    }

    @Override
    public void visit(InsertCommand command) {
        RemoteFactHandle remoteFH = command.getFactHandle();
        FactHandle fh = kieSessionContext.getKieSession().getEntryPoint(command.getEntryPoint()).insert(remoteFH.getObject());
        kieSessionContext.getFhManager().registerHandle(remoteFH, fh);
        if (firingUntilHalt) {
            kieSessionContext.getKieSession().fireAllRules();
        }
    }

    @Override
    public void visit(EventInsertCommand command) {
        FactHandle fh = kieSessionContext.getKieSession().getEntryPoint(command.getEntryPoint()).insert(command.getObject());
        if (firingUntilHalt) {
            kieSessionContext.getKieSession().fireAllRules();
        }
    }

    @Override
    public void visit(DeleteCommand command) {
        FactHandle factHandle = kieSessionContext.getFhManager().mapRemoteFactHandle(command.getFactHandle());
        kieSessionContext.getKieSession().getEntryPoint(command.getEntryPoint()).delete(factHandle);
        if (firingUntilHalt) {
            kieSessionContext.getKieSession().fireAllRules();
        }
    }

    @Override
    public void visit(UpdateCommand command) {
        FactHandle factHandle = kieSessionContext.getFhManager().mapRemoteFactHandle(command.getFactHandle());
        kieSessionContext.getKieSession().getEntryPoint(command.getEntryPoint()).update(factHandle, command.getObject());
        if (firingUntilHalt) {
            kieSessionContext.getKieSession().fireAllRules();
        }
    }

    @Override
    public void visit(ListObjectsCommand command) {
        List serializableItems = getObjectList(command);
        ListKieSessionObjectMessageImpl msg = new ListKieSessionObjectMessageImpl(command.getId(), serializableItems);
        producer.produceSync(config.getKieSessionInfosTopicName(), command.getId(), msg);
    }

    private List getObjectList(ListObjectsCommand command) {
        Collection<? extends Object> objects = kieSessionContext.getKieSession().getEntryPoint(command.getEntryPoint()).getObjects();
        return getListFromSerializableCollection(objects);
    }

    @Override
    public void visit(ListObjectsCommandClassType command) {
        List serializableItems = getSerializableItemsByClassType(command);
        ListKieSessionObjectMessageImpl msg = new ListKieSessionObjectMessageImpl(command.getId(), serializableItems);
        producer.produceSync(config.getKieSessionInfosTopicName(),
                             command.getId(),
                             msg);
    }

    private List getSerializableItemsByClassType(ListObjectsCommandClassType command) {
        Collection<? extends Object> objects = ObjectFilterHelper.getObjectsFilterByClassType(command.getClazzType(),
                                                                                              kieSessionContext.getKieSession());
        return getListFromSerializableCollection(objects);
    }

    private List getListFromSerializableCollection(Collection<?> objects) {
        List serializableItems = new ArrayList<>(objects.size());
        Iterator<? extends Object> iterator = objects.iterator();
        while (iterator.hasNext()) {
            Object o = iterator.next();
            serializableItems.add(o);
        }
        return serializableItems;
    }

    @Override
    public void visit(ListObjectsCommandNamedQuery command) {
        List serializableItems = getSerializableItemsByNamedQuery(command);
        ListKieSessionObjectMessageImpl msg = new ListKieSessionObjectMessageImpl(command.getId(), serializableItems);
        producer.produceSync(config.getKieSessionInfosTopicName(),
                             command.getId(),
                             msg);
    }

    private List getSerializableItemsByNamedQuery(ListObjectsCommandNamedQuery command) {
        Collection<? extends Object> objects = ObjectFilterHelper.getObjectsFilterByNamedQuery(command.getNamedQuery(),
                                                                                               command.getObjectName(),
                                                                                               command.getParams(),
                                                                                               kieSessionContext.getKieSession());
        return getListFromSerializableCollection(objects);
    }

    @Override
    public void visit(FactCountCommand command) {
        FactCountMessageImpl msg = new FactCountMessageImpl(command.getId(),
                                                            kieSessionContext.getKieSession().getFactCount());
        producer.produceSync(config.getKieSessionInfosTopicName(),
                             command.getId(),
                             msg);
    }
}
