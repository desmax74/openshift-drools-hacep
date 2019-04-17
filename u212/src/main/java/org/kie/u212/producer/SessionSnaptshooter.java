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
package org.kie.u212.producer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kie.api.KieServices;
import org.kie.api.marshalling.KieMarshallers;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.u212.Config;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionSnaptshooter<T> {

    private EventProducer<Byte[]> producer;
    private KafkaConsumer<String, Byte[]> consumer;
    private Properties configuration;
    private  KieContainer kieContainer;
    private final String key = "LAST-SNAPSHOT";

    private final Logger logger = LoggerFactory.getLogger(SessionSnaptshooter.class);

    public SessionSnaptshooter(Properties config){
        configuration =config;
        kieContainer = KieServices.get().newKieClasspathContainer();
        producer = new EventProducer<>();
        producer.start(configuration);
        configConsumer();
    }


    public void serialize(KieSession kSession){
        logger.info("I'm serializing session !");
        KieMarshallers marshallers = KieServices.get().getMarshallers();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            marshallers.newMarshaller( kSession.getKieBase() ).marshall( out, kSession );
            EventWrapper wrapper = new EventWrapper(out.toByteArray(),key, 0l, EventType.SNAPSHOT);
            producer.produceSync(new ProducerRecord(wrapper.getKey(), wrapper));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

    }


    public KieSession deserialize(){
        KieMarshallers marshallers = KieServices.get().getMarshallers();
        KieSession kSession = null;
        ConsumerRecords<String, Byte[]> records = consumer.poll(Duration.of(Integer.valueOf(Config.DEFAULT_POLL_TIMEOUT_MS), ChronoUnit.MILLIS));
        for (ConsumerRecord record : records) {
            EventWrapper wrapper = (EventWrapper) record.value();
            try (ByteArrayInputStream in = new ByteArrayInputStream((byte[])wrapper.getDomainEvent())) {
                kSession = marshallers.newMarshaller( kieContainer.getKieBase() ).unmarshall( in );
            } catch (IOException | ClassNotFoundException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return kSession;
    }


    private void configConsumer() {
        consumer = new KafkaConsumer(configuration);
        List<PartitionInfo> partitionsInfo = consumer.partitionsFor(Config.SNAPSHOT_TOPIC);
        List<TopicPartition> partitions = new ArrayList<>();
        Collection<TopicPartition> partitionCollection = new ArrayList<>();

        if (partitionsInfo != null) {
            for (PartitionInfo partition : partitionsInfo) {
                if (partitions == null || partitions.contains(partition.partition())) {
                    partitionCollection.add(new TopicPartition(partition.topic(), partition.partition()));
                }
            }

            if (!partitionCollection.isEmpty()) {
                consumer.assign(partitionCollection);
            }
        }
        consumer.assignment().forEach(topicPartition -> consumer.seekToBeginning(partitionCollection));
    }

}
