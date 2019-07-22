package org.kie.remote.message;

import java.util.Collection;

public interface ListKieSessionObjectMessage {

    String getKey();

    Collection<? extends Object> getObjects();

    void accept(VisitorMessage visitor);
}
