package org.kie.remote;

public interface RemoteEventKieSession<T> extends RemoteCepEntryPoint {

    RemoteCepEntryPoint getEntryPoint(String name);
}
