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

import java.util.Queue;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.time.SessionPseudoClock;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.KieSessionContext;
import org.kie.hacep.core.infra.DeafultSessionSnapShooter;
import org.kie.hacep.core.infra.SnapshotInfos;
import org.kie.hacep.core.infra.consumer.ConsumerHandler;
import org.kie.hacep.core.infra.consumer.ItemToProcess;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.core.infra.utils.SnapshotOnDemandUtils;
import org.kie.hacep.message.ControlMessage;
import org.kie.hacep.util.PrinterUtil;
import org.kie.remote.DroolsExecutor;
import org.kie.remote.command.RemoteCommand;
import org.kie.remote.command.VisitableCommand;
import org.kie.remote.impl.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kie.remote.util.SerializationUtil.deserialize;

public class DroolsConsumerHandler implements ConsumerHandler {

    private static final Logger logger = LoggerFactory.getLogger(DroolsConsumerHandler.class);
    private Logger loggerForTest;
    private SessionPseudoClock clock;
    private Producer producer;
    private DeafultSessionSnapShooter snapshooter;
    private EnvConfig config;
    private KieSessionContext kieSessionContext;
    private CommandHandler commandHandler;
    private SnapshotInfos infos;
    private boolean shutdown;

    public DroolsConsumerHandler(Producer producer, EnvConfig envConfig) {
        this.config = envConfig;
        this.snapshooter = new DeafultSessionSnapShooter(config);
        initializeKieSessionFromSnapshot(config);
        this.producer = producer;
        commandHandler = new CommandHandler(kieSessionContext, config, producer, snapshooter);
        if (config.isUnderTest()) {
            loggerForTest = PrinterUtil.getKafkaLoggerForTest(envConfig);
        }
    }

    private void initializeKieSessionFromSnapshot(EnvConfig config) {
        if(config.isSkipOnDemanSnapshot()) {// if true we reads the snapshots and waitn until the first leaderElectionUpdate
            this.infos = snapshooter.deserialize();
            this.kieSessionContext = createSessionHolder(infos);
            clock = kieSessionContext.getKieSession().getSessionClock();//@TODO Mario
            //clock.advanceTime(stock.getTimestamp() - clock.getCurrentTime(), TimeUnit.MILLISECONDS); //@TODO Mario
        } else{
            kieSessionContext = new KieSessionContext();
            createClasspathSession( kieSessionContext );
        }
    }

    public boolean initializeKieSessionFromSnapshotOnDemand(EnvConfig config) {
        if(!config.isSkipOnDemanSnapshot()) {// if true we reads the snapshots and wait until the first leaderElectionUpdate
            this.infos = SnapshotOnDemandUtils.askASnapshotOnDemand(config, snapshooter);
            this.kieSessionContext = createSessionHolder(infos);
            clock = kieSessionContext.getKieSession().getSessionClock();//@TODO Mario
            //clock.advanceTime(stock.getTimestamp() - clock.getCurrentTime(), TimeUnit.MILLISECONDS); //@TODO Mario
            return true;
        }
        return false;
    }

    public DeafultSessionSnapShooter getSnapshooter(){
        return snapshooter;
    }

    @Override
    public void process( ItemToProcess item, State state) {
        RemoteCommand command  = deserialize((byte[])item.getObject());
        process( command, state );
    }

    @Override
    public void process( RemoteCommand command, State state ) {
        if(config.isUnderTest()) {  loggerForTest.warn("Remote command on process:{}", command); }
        if (state.equals(State.LEADER)) {
            processCommand( command, state );
            Queue<Object> sideEffectsResults = DroolsExecutor.getInstance().getAndReset();
            ControlMessage newControlMessage = new ControlMessage(command.getId(), sideEffectsResults);
            producer.produceSync(config.getControlTopicName(), command.getId(), newControlMessage);
            if (config.isUnderTest()) { loggerForTest.warn("sideEffectOnLeader:{}", sideEffectsResults); }
        } else {
            processCommand( command, state );
        }
    }

    public void processSideEffectsOnReplica(Queue<Object> newSideEffects) {
        DroolsExecutor.getInstance().appendSideEffects(newSideEffects);
        if(config.isUnderTest()){ loggerForTest.warn("sideEffectOnReplica:{}", newSideEffects);}
    }

    @Override
    public void processWithSnapshot(ItemToProcess item, State currentState) {
        if (logger.isInfoEnabled()){ logger.info("SNAPSHOT"); }
        process(item, currentState);
        if(!shutdown) {
            snapshooter.serialize(kieSessionContext, item.getKey(), item.getOffset());
        }
    }

    @Override
    public void stop() {
        shutdown = true;
        if(kieSessionContext != null) {
            kieSessionContext.getKieSession().dispose();
        }
    }

    private void processCommand( RemoteCommand command, State state ) {
        boolean execute = state.equals(State.LEADER) || command.isPermittedForReplicas();
        if (execute) {
            VisitableCommand visitable = (VisitableCommand) command;
            visitable.accept(commandHandler);
        }
    }

    private KieSessionContext createSessionHolder(SnapshotInfos infos ) {
        KieSessionContext kieSessionContext = new KieSessionContext();
        if (infos != null) {
            if(logger.isInfoEnabled()){ logger.info("start consumer with:{}", infos);}
            initSessionHolder( infos, kieSessionContext );
        } else {
            createClasspathSession( kieSessionContext );
        }
        return kieSessionContext;
    }

    private void createClasspathSession( KieSessionContext kieSessionContext ) {
        KieServices srv = KieServices.get();
        if (srv != null) {
            if (logger.isInfoEnabled()) {logger.info("Creating new Kie Session");}
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            kieSessionContext.init(kieContainer.newKieSession());
        } else {
            logger.error("KieService is null");
        }
    }

    private void initSessionHolder(SnapshotInfos infos, KieSessionContext kieSessionContext) {
        if (infos.getKieSession() == null) {
            KieContainer kieContainer = KieServices.get().newKieClasspathContainer();
            kieSessionContext.init(kieContainer.newKieSession());
        } else {
            if(logger.isInfoEnabled()){ logger.info("Applying snapshot");}
            kieSessionContext.initFromSnapshot(infos);
        }
    }
}