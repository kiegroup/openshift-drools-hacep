package org.kie.remote.command;

import java.io.Serializable;
import java.util.UUID;

public class PoisonPill extends AbstractCommand implements VisitableCommand, Serializable {

    public PoisonPill() {
        super(UUID.randomUUID().toString());
    }

    @Override
    public void accept(VisitorCommand visitor) { visitor.visit(this); }

    @Override
    public boolean isPermittedForReplicas() { return true; }

    @Override
    public String toString() {
        return "PoisonPill of " + getId();
    }
}
