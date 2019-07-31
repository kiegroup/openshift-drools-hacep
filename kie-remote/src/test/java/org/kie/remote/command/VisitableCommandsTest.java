package org.kie.remote.command;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class VisitableCommandsTest {

    @Test
    public void testDeleteCommandAcceptMethod() {
        final DeleteCommand deleteCommand = new DeleteCommand();
        final TestVisitor testVisitor = new TestVisitor(deleteCommand);
        testVisitor.visit(deleteCommand);
    }

    @Test
    public void testEventInsertCommandAcceptMethod() {
        final EventInsertCommand eventInsertCommand = new EventInsertCommand();
        final TestVisitor testVisitor = new TestVisitor(eventInsertCommand);
        testVisitor.visit(eventInsertCommand);
    }

    @Test
    public void testFactCountCommandAcceptMethod() {
        final FactCountCommand factCountCommand = new FactCountCommand();
        final TestVisitor testVisitor = new TestVisitor(factCountCommand);
        testVisitor.visit(factCountCommand);
    }

    @Test
    public void testFireAllRulesCommandAcceptMethod() {
        final FireAllRulesCommand fireAllRulesCommand = new FireAllRulesCommand();
        final TestVisitor testVisitor = new TestVisitor(fireAllRulesCommand);
        testVisitor.visit(fireAllRulesCommand);
    }

    @Test
    public void testFireUntilHaltCommandAcceptMethod() {
        final FireUntilHaltCommand fireUntilHaltCommand = new FireUntilHaltCommand();
        final TestVisitor testVisitor = new TestVisitor(fireUntilHaltCommand);
        testVisitor.visit(fireUntilHaltCommand);
    }

    @Test
    public void testHaltCommandAcceptMethod() {
        final HaltCommand haltCommand = new HaltCommand();
        final TestVisitor testVisitor = new TestVisitor(haltCommand);
        testVisitor.visit(haltCommand);
    }

    @Test
    public void testUpdateCommandAcceptMethod() {
        final UpdateCommand updateCommand = new UpdateCommand();
        final TestVisitor testVisitor = new TestVisitor(updateCommand);
        testVisitor.visit(updateCommand);
    }

    @Test
    public void testListObjectsCommandAcceptMethod() {
        final ListObjectsCommand listObjectsCommand = new ListObjectsCommand();
        final TestVisitor testVisitor = new TestVisitor(listObjectsCommand);
        testVisitor.visit(listObjectsCommand);
    }

    @Test
    public void testListObjectsCommandClassTypeAcceptMethod() {
        final ListObjectsCommandClassType listObjectsCommandClassType = new ListObjectsCommandClassType();
        final TestVisitor testVisitor = new TestVisitor(listObjectsCommandClassType);
        testVisitor.visit(listObjectsCommandClassType);
    }

    @Test
    public void testListObjectsCommandNamedQueryAcceptMethod() {
        final ListObjectsCommandNamedQuery listObjectsCommandNamedQuery = new ListObjectsCommandNamedQuery();
        final TestVisitor testVisitor = new TestVisitor(listObjectsCommandNamedQuery);
        testVisitor.visit(listObjectsCommandNamedQuery);
    }

    @Test
    public void testInsertCommandAcceptMethod() {
        final InsertCommand insertCommand = new InsertCommand();
        final TestVisitor testVisitor = new TestVisitor(insertCommand);
        testVisitor.visit(insertCommand);
    }

    @Test
    public void testSnapshotOnDemandCommandAcceptMethod() {
        final SnapshotOnDemandCommand insertCommand = new SnapshotOnDemandCommand();
        final TestVisitor testVisitor = new TestVisitor(insertCommand);
        testVisitor.visit(insertCommand);
    }

    private class TestVisitor implements VisitorCommand {

        private VisitableCommand visitableCommand;

        public TestVisitor(VisitableCommand visitableCommand) {
            this.visitableCommand = visitableCommand;
        }

        @Override
        public void visit(FireAllRulesCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(FireUntilHaltCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(HaltCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(InsertCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(EventInsertCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(DeleteCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(UpdateCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(ListObjectsCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(ListObjectsCommandClassType command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(ListObjectsCommandNamedQuery command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(SnapshotOnDemandCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }

        @Override
        public void visit(FactCountCommand command) {
            Assertions.assertThat(visitableCommand).isEqualTo(command);
        }
    }
}
