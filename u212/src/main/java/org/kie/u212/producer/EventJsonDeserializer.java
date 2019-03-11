package org.kie.u212.producer;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.kie.u212.consumer.EventJsonSerializer;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventJsonDeserializer implements Deserializer<StockTickEvent> {

  private Logger logger = LoggerFactory.getLogger(EventJsonSerializer.class);

  private ObjectMapper objectMapper;

  @Override
  public void configure(Map configs, boolean isKey) {
    this.objectMapper = new ObjectMapper();
  }


  @Override
  public StockTickEvent deserialize(String s, byte[] data) {
    try {
      return objectMapper.readValue(data, StockTickEvent.class);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public void close() { }
}
