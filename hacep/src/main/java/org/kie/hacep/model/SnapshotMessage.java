/*
 * Copyright 2019 Red Hat
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

package org.kie.hacep.model;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.kie.remote.RemoteFactHandle;

public class SnapshotMessage implements Serializable {
    private byte[] serializedSession;
    private transient Set<RemoteFactHandle> keys;
    private String lastInsertedEventkey;
    private long lastInsertedEventOffset;

    /* Empty constructor for serialization */
    public SnapshotMessage() { }

    public SnapshotMessage(byte[] serializedSession, Set<RemoteFactHandle> fhMapKeys, String lastInsertedEventkey, long lastInsertedEventOffset ) {
        this.serializedSession = serializedSession;
        this.keys = fhMapKeys;
        this.lastInsertedEventkey = lastInsertedEventkey;
        this.lastInsertedEventOffset = lastInsertedEventOffset;
    }

    public byte[] getSerializedSession() {
        return serializedSession;
    }

    public void setSerializedSession( byte[] serializedSession ) {
        this.serializedSession = serializedSession;
    }

    public Set<RemoteFactHandle> getFhMapKeys() {
        return keys;
    }

    public void setFhMapKeys( Set<RemoteFactHandle> fhMapKeys ) {
        this.keys = fhMapKeys;
    }

    public String getLastInsertedEventkey() {
        return lastInsertedEventkey;
    }

    public void setLastInsertedEventkey( String lastInsertedEventkey ) {
        this.lastInsertedEventkey = lastInsertedEventkey;
    }

    public long getLastInsertedEventOffset() {
        return lastInsertedEventOffset;
    }

    public void setLastInsertedEventOffset( long lastInsertedEventOffset ) {
        this.lastInsertedEventOffset = lastInsertedEventOffset;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SnapshotMessage{");
        sb.append("serializedSession=bytes[]");
        sb.append(", fhMapKeys=").append(keys);
        sb.append(", lastInsertedEventkey='").append(lastInsertedEventkey).append('\'');
        sb.append(", lastInsertedEventOffset=").append(lastInsertedEventOffset);
        sb.append('}');
        return sb.toString();
    }

    /* Methods for serialization */
    private void writeObject(ObjectOutputStream s) throws ClassNotFoundException, IOException {
        s.defaultWriteObject();
        s.writeInt(keys.size());
        Iterator iterator = keys.iterator();
        while(iterator.hasNext()){
            s.writeObject(iterator.next());
        }
    }

    private void readObject(ObjectInputStream s) throws ClassNotFoundException, IOException{
        s.defaultReadObject();
        int keysSize = s.readInt();
        Set<RemoteFactHandle> a = keys = new HashSet<>(keysSize);
        for(int i =0; i<keysSize; i++){
           a.add((RemoteFactHandle) s.readObject());
        }
    }
}
