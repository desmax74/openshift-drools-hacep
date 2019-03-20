package org.kie.u212;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.u212.core.infra.producer.ProducerCallbackLog;
import org.kie.u212.model.StockTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTest {


  private static Logger logger = LoggerFactory.getLogger(ProducerCallbackLog.class);

  public static void main(String[] args) throws Exception {

    String topic="user";
    String kafkaRoute = "my-kafka-brokers-my-kafka-project.192.168.42.74.nip.io:9091";//replace with your kafka address


    Client client = new Client(kafkaRoute, topic);
    client.start();

    StockTickEvent eventA = new StockTickEvent("RHT", ThreadLocalRandom.current().nextLong(80, 100), UUID.randomUUID().toString());
    client.insertSync(eventA, true);
    logger.info("Insert EventA");
    StockTickEvent eventB = new StockTickEvent("RHT", ThreadLocalRandom.current().nextLong(80, 100), UUID.randomUUID().toString());
    ProducerCallbackLog producerCallback = new ProducerCallbackLog();
    client.insertAsync(eventB, producerCallback);
    logger.info("Insert EventB");
    StockTickEvent eventC = new StockTickEvent("RHT", ThreadLocalRandom.current().nextLong(80, 100), UUID.randomUUID().toString());
    Future<RecordMetadata> futureRecord = client.insertFireAndForget(eventC);
    RecordMetadata last = futureRecord.get();
    logger.info("Insert EventC");
    client.close();
  }
}
