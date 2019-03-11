package org.kie.u212.consumer;

import org.kie.u212.PubSubConfig;

public class DroolsEventCosumerApp {

    private DroolsConsumerController consumerController = new DroolsConsumerController();

    public void businessLogic(){
        consumerController.consumeEvents(PubSubConfig.GROUP, -1, 10);
    }
}
