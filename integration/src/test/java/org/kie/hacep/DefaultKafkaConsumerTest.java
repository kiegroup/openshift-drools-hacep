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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.Bootstrap;
import org.kie.hacep.core.infra.election.State;

public class DefaultKafkaConsumerTest {


    @Before
    public void initTest() {
        EnvConfig config = EnvConfig.getDefaultEnvConfig().underTest(true).local(true);
        Bootstrap.startEngine(config);
        Bootstrap.getConsumerController().getCallback().updateStatus(State.LEADER);
    }

    @After
    public void endTest() {
        Bootstrap.stopEngine();
    }

    @Test
    public void test(){
        System.out.println("Do something");
    }
}
