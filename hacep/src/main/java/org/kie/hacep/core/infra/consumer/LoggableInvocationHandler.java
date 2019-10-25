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
package org.kie.hacep.core.infra.consumer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggableInvocationHandler implements InvocationHandler {


    private Logger kafkaLogger = LoggerFactory.getLogger("testx.org.hacep");
    private Object target;


    public LoggableInvocationHandler(Object target){
        this.target = target;
    }

    public static Object createProxy(Object target){
        return Proxy.newProxyInstance(target.getClass().getClassLoader(),
                                      target.getClass().getInterfaces(),
                                      new LoggableInvocationHandler(target));
    }

    @Override
    public Object invoke(Object proxy,
                         Method method,
                         Object[] args) throws Throwable {

        Object result = method.invoke(target, args);
        if(!method.getDeclaringClass().equals(EventConsumerStatus.class)){
            kafkaLogger.warn("{}.{}", method.getDeclaringClass(),method.getName());
        }
        return result;
    }
}
