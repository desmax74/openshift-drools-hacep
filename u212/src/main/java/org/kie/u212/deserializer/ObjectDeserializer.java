package org.kie.u212.deserializer;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.kie.u212.model.EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectDeserializer implements Deserializer<EventWrapper> {

  private Logger logger = LoggerFactory.getLogger(ObjectDeserializer.class);

  private ObjectMapper objectMapper;

  @Override
  public void configure(Map configs, boolean isKey) {
    this.objectMapper = new ObjectMapper();
  }

  @Override
  public EventWrapper deserialize(String s, byte[] data) {
    try {
      return objectMapper.readValue(data, EventWrapper.class);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public void close() {}
}
