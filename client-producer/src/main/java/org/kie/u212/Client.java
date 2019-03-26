package org.kie.u212;

import java.io.IOException;
import java.io.InputStream;
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
    InputStream in = null;
    try {
      in = this.getClass().getClassLoader().getResourceAsStream("configuration.properties");
    }catch (Exception e){
    }finally {
      try{
      props.load(in);
      in.close();
      }catch (IOException ioe){}
    }

    return  props;
  }
}
