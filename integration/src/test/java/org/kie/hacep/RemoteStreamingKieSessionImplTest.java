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
package org.kie.hacep;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.CommonConfig;
import org.kie.remote.RemoteStreamingKieSession;
import org.kie.remote.TopicsConfig;
import org.kie.remote.impl.RemoteStreamingKieSessionImpl;

import static org.junit.Assert.*;

@Ignore
public class RemoteStreamingKieSessionImplTest {

    private KafkaUtilTest kafkaServerTest;
    private EnvConfig config;
    private TopicsConfig topicsConfig;

    @Before
    public void setUp() throws Exception {
        config = EnvConfig.getDefaultEnvConfig();
        topicsConfig = TopicsConfig.getDefaultTopicsConfig();
        kafkaServerTest = new KafkaUtilTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopics(config.getEventsTopicName(),
                                     config.getControlTopicName(),
                                     config.getSnapshotTopicName(),
                                     config.getKieSessionInfosTopicName());
    }

    @After
    public void tearDown() {
        try {
            Bootstrap.stopEngine();
        } catch (ConcurrentModificationException ex) {
        }
        kafkaServerTest.shutdownServer();
    }

    @Test
    public void getFactCountTest() throws Exception {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(7,
                                                    topicsConfig,
                                                    RemoteStreamingKieSession.class);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(Config.getProducerConfig("FactCountConsumerTest"),
                                                                     topicsConfig);
        try {
            client.listen();
            CompletableFuture<Long> factCountFuture = client.getFactCount();
            Long factCount = factCountFuture.get(5, TimeUnit.SECONDS);
            assertTrue(factCount == 7);
        }finally {
            client.close();
        }
    }

    @Test
    public void getListKieSessionObjectsTest() throws Exception {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    topicsConfig,
                                                    RemoteStreamingKieSession.class);

        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(CommonConfig.getProducerConfig("ListKieSessionObjectsConsumerTest"),
                                                                     topicsConfig);
        try {
            client.listen();
            CompletableFuture<Collection<? extends Object>> listKieObjectsFuture = client.getObjects();
            Collection<? extends Object> listKieObjects = listKieObjectsFuture.get(5,
                                                                                   TimeUnit.SECONDS);
            assertTrue(listKieObjects.size() == 1);
            StockTickEvent event = (StockTickEvent) listKieObjects.iterator().next();
            assertTrue(event.getCompany().equals("RHT"));
        }finally {
            client.close();
        }

    }

    @Test
    public void getListKieSessionObjectsWithClassTypeTest() throws Exception {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    topicsConfig,
                                                    RemoteStreamingKieSession.class);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsWithClassTypeTest"),
                                                                     topicsConfig);
        try {
            client.listen();
            CompletableFuture<Collection<? extends Object>> listKieObjectsFuture = client.getObjects(StockTickEvent.class);
            Collection<? extends Object> listKieObjects = listKieObjectsFuture.get(5, TimeUnit.SECONDS);
            assertTrue(listKieObjects.size() == 1);
            StockTickEvent event = (StockTickEvent) listKieObjects.iterator().next();
            assertTrue(event.getCompany().equals("RHT"));
        }finally {
            client.close();
        }
    }

    @Test
    public void getListKieSessionObjectsWithNamedQueryTest() throws Exception {
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
        kafkaServerTest.insertBatchStockTicketEvent(1,
                                                    topicsConfig,
                                                    RemoteStreamingKieSession.class);
        RemoteStreamingKieSessionImpl client = new RemoteStreamingKieSessionImpl(Config.getProducerConfig("ListKieSessionObjectsWithNamedQueryTest"),
                                                                     topicsConfig);
        try{
            client.listen();

            doQuery( client, "IBM", 0 );

            Collection<?> listKieObjects = doQuery( client, "RHT", 1 );
            StockTickEvent event = (StockTickEvent)listKieObjects.iterator().next();
            assertTrue(event.getCompany().equals("RHT"));
        }finally {
            client.close();
        }
    }

    private Collection<?> doQuery( RemoteStreamingKieSessionImpl client, String stockName, int expectedResult) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        CompletableFuture<Collection<?>> listKieObjectsFuture;
        Collection<?> listKieObjects;
        listKieObjectsFuture = client.getObjects("stockTickEventQuery" , "stock", stockName);
        listKieObjects = listKieObjectsFuture.get(5, TimeUnit.SECONDS);
        assertEquals(expectedResult, listKieObjects.size());
        return listKieObjects;
    }

}
