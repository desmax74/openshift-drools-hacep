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

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.kie.u212.model.EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventJsonSerializer implements Serializer<EventWrapper> {

    private Logger logger = LoggerFactory.getLogger(EventJsonSerializer.class);

    @Override
    public void configure(Map<String, ?> configs,
                          boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic,
                            EventWrapper data) {
        byte[] output = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            output = mapper.writeValueAsString(data).getBytes();
        } catch (Exception exception) {
            logger.error("Error in serialize {} \n {} \n {} \n",
                         data,
                         exception.getMessage(),
                         exception);
        }
        return output;
    }

    @Override
    public void close() {
    }
}
