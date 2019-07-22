package org.kie.remote.message;

public interface FactCountMessage {

    String getKey();

    long getFactCount();

    void accept(VisitorMessage visitor);
}
