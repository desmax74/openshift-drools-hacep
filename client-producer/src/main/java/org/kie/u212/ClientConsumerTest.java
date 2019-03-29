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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConsumerTest {

    private static Logger logger = LoggerFactory.getLogger(ClientConsumerTest.class);

    public static void main(String[] args) {
        Properties props = getConfiguration();

        EventWrapper wrapper = ConsumerUtils.getLastEvent(Config.CONTROL_TOPIC,
                                                          props);
        logger.info("EventWrpper:{}", wrapper);

    }

    private static Properties getConfiguration() {
        Properties props = new Properties();
        InputStream in = null;
        try {
            in = ClientConsumerTest.class.getClassLoader().getResourceAsStream("configuration.properties");
        } catch (Exception e) {
        } finally {
            try {
                props.load(in);
                in.close();
            } catch (IOException ioe) {
            }
        }

        return props;
    }
}
