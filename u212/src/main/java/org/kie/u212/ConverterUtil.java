/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
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
package org.kie.u212;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.kie.remote.RemoteCommand;
import org.kie.remote.command.AbstractCommand;
import org.kie.remote.command.DeleteCommand;
import org.kie.remote.command.InsertCommand;
import org.kie.remote.command.ListObjectsCommand;
import org.kie.remote.command.WorkingMemoryActionCommand;
import org.kie.u212.model.ControlMessage;
import org.kie.u212.model.FactCountMessage;
import org.kie.u212.model.SnapshotMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConverterUtil {

    private static Logger logger = LoggerFactory.getLogger(ConverterUtil.class);

    private static List<Class> managedTypes = initMagedTypes();

    private static List<Class> initMagedTypes() {
        List<Class> managedTypes = new ArrayList<>();

        managedTypes.add(ControlMessage.class);
        managedTypes.add(SnapshotMessage.class);
        managedTypes.add(FactCountMessage.class);

        managedTypes.add(RemoteCommand.class);
        managedTypes.add(InsertCommand.class);
        managedTypes.add(DeleteCommand.class);
        managedTypes.add(FactCountMessage.class);
        managedTypes.add(ListObjectsCommand.class);
        managedTypes.add(AbstractCommand.class);
        managedTypes.add(WorkingMemoryActionCommand.class);

        return managedTypes;
    }

    public static void addManagedType(Class clazz){
        managedTypes.add(clazz);
    }

    public static byte[] serializeObj(Object obj) {
        try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
            try (ObjectOutputStream o = new ObjectOutputStream(b)) {
                o.writeObject(obj);
            }
            return b.toByteArray();
        } catch (IOException io) {
            logger.error(io.getMessage(),
                         io);
        }
        return new byte[]{};
    }

    public static Object deSerializeObj(byte[] bytez) {
        Object msg = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytez);
            ObjectInput in = new ObjectInputStream(bis);
            msg = in.readObject();
        } catch (Exception e) {
            logger.error(e.getMessage(),
                         e);
        }
        return msg;
    }

    public static <T> T deSerializeObjInto(byte[] bytez, Class<T> type) {
        if (managedTypes.contains(type)) {
            Object msg = null;
            try {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytez);
                ObjectInput in = new ObjectInputStream(bis);
                msg = in.readObject();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return type.cast(msg);
        } else {
            return (T) new Object();
        }
    }
}
