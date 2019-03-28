package org.kie.u212;

import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConsumer {


  private static Logger logger = LoggerFactory.getLogger(ClientConsumer.class);

  public static void main(String[] args) {
    KafkaConsumer consumer = new KafkaConsumer(getConfiguration());
    consumer.subscribe(Arrays.asList(Config.CONTROL_TOPIC));
    getOffset(consumer, Config.CONTROL_TOPIC);
  }

  private static void getOffset(KafkaConsumer consumer, String topic) {
    List<PartitionInfo> infos = consumer.partitionsFor(topic);
    List<TopicPartition> tps = new ArrayList<>();
    for(PartitionInfo info : infos){
      tps.add(new TopicPartition(topic,info.partition()));
    }
    Map<TopicPartition, Long> offsets = consumer.endOffsets(tps);
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      logger.info(entry.getKey() + ":" + entry.getValue());
    }
  }

  private static Properties getConfiguration(){
    Properties props = new Properties();
    InputStream in = null;
    try {
      in = ClientConsumer.class.getClassLoader().getResourceAsStream("configuration.properties");
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
