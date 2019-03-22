package org.kie.u212;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.u212.core.Core;
import org.kie.u212.core.infra.producer.EventProducer;
import org.kie.u212.core.infra.utils.RecordMetadataUtil;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements AutoCloseable{

  private static Logger logger = LoggerFactory.getLogger(Client.class);
  private EventProducer producer;
  private Properties properties;
  private String topic;

  public Client(String topic){
    producer = new EventProducer<>();
    properties = new Properties();
    properties = getConfiguration();
    this.topic = topic;
  }

  public void start(){
    logger.info("Start client producer");
    producer.start(properties);
  }

  @Override
  public void close() {
    logger.info("Closing client producer");
    producer.stop();
  }

  public RecordMetadata insertSync(StockTickEvent event, boolean logInsert){
    RecordMetadata lastRecord = producer.produceSync(new ProducerRecord<>(topic, event.getId(), event));
    if(logInsert) {
      RecordMetadataUtil.logRecord(lastRecord);
    }
    return lastRecord;
  }

  public void insertAsync(StockTickEvent event, Callback callback){
    producer.produceAsync(new ProducerRecord<>(topic, event.getId(), event), callback);
  }

  public Future<RecordMetadata> insertFireAndForget(StockTickEvent event){
    return producer.produceFireAndForget(new ProducerRecord<>(topic, event.getId(), event));
  }

  private Properties getConfiguration(){
    Properties props = new Properties();
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.kie.u212.consumer.EventJsonSerializer");
    props.put("bootstrap.servers", "<bootstrap.server_url>:443");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.kie.u212.producer.EventJsonDeserializer");
    props.put("max.poll.interval.ms", "10000");//time to discover the new consumer after a changetopic default 5 min 300000
    props.put("metadata.max.age.ms", "10000");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("security.protocol", "SSL");
    props.put("ssl.keystore.location", "<path>/src/main/resources/keystore.jks");
    props.put("ssl.keystore.password", "password");
    props.put("ssl.truststore.location", "<path>/src/main/resources/keystore.jks");
    props.put("ssl.truststore.password", "password");
    return  props;
  }
}
