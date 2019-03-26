package org.kie.u212.core.infra.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.kie.u212.model.EventWrapper;
import org.kie.u212.model.EventWrapperImpl;
import org.kie.u212.model.StockTickEvent;

public class HelperConsumer<T> {

  private Properties properties;

  public HelperConsumer(Properties configuration){
      properties = configuration;
  }

  public EventWrapper<T> getLastEventFromTopic(String topic){
    StockTickEvent lastEvent = null;
    KafkaConsumer consumer = new KafkaConsumer(properties);
    consumer.subscribe(Arrays.asList(topic));
    consumer.seekToEnd(consumer.assignment());
    ConsumerRecords<String, T> records = consumer.poll(10);
    long lastOffset = 0l;
    for (ConsumerRecord<String, T> record : records) {
      lastOffset = record.offset();
      lastEvent = (StockTickEvent)record.value();
    }
    consumer.close();
    return new EventWrapperImpl<>(lastEvent, lastEvent.getId(), lastOffset);
  }

}
