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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;
import org.kie.hacep.sample.kjar.Result;
import org.kie.hacep.sample.kjar.StockTickEvent;
import org.kie.remote.RemoteKieSession;

import static org.junit.Assert.assertEquals;
import static org.kie.remote.CommonConfig.getTestProperties;

public class LocalMessagesSessionTest {

    @Test
    public void test() throws ExecutionException, InterruptedException, IOException {
        EnvConfig config = EnvConfig.getDefaultEnvConfig().underTest( true ).local( true );

        Bootstrap.startEngine( config );
        Bootstrap.getConsumerController().getCallback().updateStatus( State.LEADER );

        RemoteKieSession session = RemoteKieSession.create(getTestProperties());

        session.insert( new Result("RHT") );
        session.insert( new StockTickEvent("RHT", 7.0) );
        session.insert( new StockTickEvent("RHT", 9.0) );
        session.insert( new StockTickEvent("RHT", 14.0) );

        session.fireAllRules();

        Result result = session.getObjects( Result.class ).get().iterator().next();
        assertEquals(10.0, result.getValue());

        session.close();
        Bootstrap.stopEngine();
    }
}
