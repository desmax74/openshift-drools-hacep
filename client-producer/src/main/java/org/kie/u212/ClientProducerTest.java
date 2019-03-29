package org.kie.u212;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.kie.u212.model.EventType;
import org.kie.u212.model.EventWrapperImpl;
import org.kie.u212.model.StockTickEvent;

public class ClientProducerTest {


  public static void main(String[] args) throws Exception {
    insertBatchEvent(2);
  }

  private static void insertBatchEvent(int items){
    Client client = new Client(Config.EVENTS_TOPIC);
    client.start();
    for(int i = 0; i<items; i++){
      StockTickEvent eventA = new StockTickEvent("RHT", ThreadLocalRandom.current().nextLong(80, 100));

      EventWrapperImpl wr = new EventWrapperImpl(eventA, UUID.randomUUID().toString(), 0l, EventType.APP);
      client.insertSync(wr, true);
    }
    client.close();
  }
}
