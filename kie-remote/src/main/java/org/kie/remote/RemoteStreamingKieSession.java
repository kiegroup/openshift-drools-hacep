package org.kie.remote;

public interface RemoteStreamingKieSession extends RemoteStreamingEntryPoint, RemoteStatefulSession {

    RemoteStreamingEntryPoint getEntryPoint( String name);
}
