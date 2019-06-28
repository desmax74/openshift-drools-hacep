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

package org.kie.u212.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.drools.core.ClassObjectFilter;
import org.kie.api.command.Command;
import org.kie.api.runtime.ExecutionResults;
import org.kie.api.runtime.KieSession;
import org.kie.internal.command.CommandFactory;

public class ObjectFilterHelper {

    //@TODO WIP
    public static Collection<? extends Object> getObjectsFilterByNamedQuery(String namedQuery, KieSession kieSession){
        List<Object> result = new ArrayList<>();
        List<Command> commands = Arrays.asList(CommandFactory.newQuery(namedQuery, namedQuery ));
        ExecutionResults results = kieSession.execute(CommandFactory.newBatchExecution(commands ));
        Collection<String> identifiers  = results.getIdentifiers();
        for(String identifier: identifiers){
            result.add(results.getValue(identifier));
        }
        return result;
    }

    public static Collection<? extends Object> getObjectsFilterByClassType(Class clazzType, KieSession kieSession){
        return kieSession.getObjects(new ClassObjectFilter(clazzType));
    }
}
