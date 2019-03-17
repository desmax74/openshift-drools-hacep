package org.kie.u212.consumer;

import org.kie.u212.core.Bootstrap;
import org.kie.u212.election.Callback;
import org.kie.u212.election.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroolsCallback implements Callback {

  private DroolsConsumer consumer;
  private static final Logger logger = LoggerFactory.getLogger(DroolsCallback.class);

  public DroolsCallback(){
  }


  public void setConsumer(DroolsConsumer newConsumer){
    logger.info("setConsumer on callback");
    this.consumer = newConsumer;
  }

  @Override
  public void updateStatus(State state) {
    logger.info("updateStatus on callback, forwad on consumer");
      consumer.updateStatus(state);
  }
}
