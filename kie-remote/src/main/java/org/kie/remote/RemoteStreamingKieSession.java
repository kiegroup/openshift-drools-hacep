package org.kie.remote;

import java.io.Closeable;

public interface RemoteStreamingKieSession extends Closeable, RemoteStreamingEntryPoint, RemoteStatefulSession {

    RemoteStreamingEntryPoint getEntryPoint( String name);
}
