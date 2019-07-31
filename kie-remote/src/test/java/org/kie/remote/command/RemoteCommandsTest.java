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

package org.kie.remote.command;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class RemoteCommandsTest {

    @Test
    public void testIsPermittedForReplicas() {
        final List<RemoteCommand> permittedForReplicas = allRemoteCommands()
                .stream()
                .filter(c -> c.isPermittedForReplicas())
                .collect(Collectors.toList());

        Assertions.assertThat(permittedForReplicas).extracting("class").contains(DeleteCommand.class);
        Assertions.assertThat(permittedForReplicas).extracting("class")
                .contains(EventInsertCommand.class);
        Assertions.assertThat(permittedForReplicas).extracting("class")
                .contains(FireAllRulesCommand.class);
        Assertions.assertThat(permittedForReplicas).extracting("class")
                .contains(FireUntilHaltCommand.class);
        Assertions.assertThat(permittedForReplicas).extracting("class").contains(HaltCommand.class);
        Assertions.assertThat(permittedForReplicas).extracting("class").contains(InsertCommand.class);
    }

    @Test
    public void testIsForbiddenForReplicas() {
        final List<RemoteCommand> forbiddenForReplicas = allRemoteCommands()
                .stream()
                .filter(c -> c.isPermittedForReplicas() == false)
                .collect(Collectors.toList());

        Assertions.assertThat(forbiddenForReplicas).extracting("class").contains(FactCountCommand.class);
        Assertions.assertThat(forbiddenForReplicas).extracting("class")
                .contains(ListObjectsCommand.class);
        Assertions.assertThat(forbiddenForReplicas).extracting("class")
                .contains(ListObjectsCommandClassType.class);
        Assertions.assertThat(forbiddenForReplicas).extracting("class")
                .contains(ListObjectsCommandNamedQuery.class);
    }

    private static List<RemoteCommand> allRemoteCommands() {
        final List<RemoteCommand> commands = new ArrayList<>();
        commands.add(new DeleteCommand());
        commands.add(new EventInsertCommand());
        commands.add(new FactCountCommand());
        commands.add(new FactCountCommand());
        commands.add(new FireAllRulesCommand());
        commands.add(new FireUntilHaltCommand());
        commands.add(new HaltCommand());
        commands.add(new InsertCommand());
        commands.add(new ListObjectsCommand());
        commands.add(new ListObjectsCommandClassType());
        commands.add(new ListObjectsCommandNamedQuery());
        commands.add(new UpdateCommand());

        return commands;
    }
}
