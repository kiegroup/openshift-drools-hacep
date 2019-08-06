package org.kie.remote;

import java.io.Closeable;
import java.util.Properties;

import org.kie.remote.impl.RemoteStreamingKieSessionImpl;

public interface RemoteStreamingKieSession extends Closeable, RemoteStreamingEntryPoint, RemoteStatefulSession {

    RemoteStreamingEntryPoint getEntryPoint( String name);

    static RemoteStreamingKieSession create( Properties configuration) {
        return new RemoteStreamingKieSessionImpl( configuration );
    }

    static RemoteStreamingKieSession create(Properties configuration, TopicsConfig envConfig) {
        return new RemoteStreamingKieSessionImpl( configuration, envConfig );
    }
}
