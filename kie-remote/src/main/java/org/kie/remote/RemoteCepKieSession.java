package org.kie.remote;

public interface RemoteCepKieSession<T> extends RemoteCepEntryPoint {

    RemoteCepEntryPoint getEntryPoint(String name);
}
