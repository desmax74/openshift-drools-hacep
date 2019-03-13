package org.kie.u212.consumer;

import org.kie.u212.core.Config;

public class DroolsEventCosumerApp {

  private DroolsConsumerController consumerController = new DroolsConsumerController();

  public void businessLogic() {
    consumerController.consumeEvents(Config.GROUP,
                                     -1,
                                     10);
  }
}
