package org.kie.u212.consumer;

import org.kie.u212.consumer.DroolsConsumerController;

public class DroolsEventCosumerApp {

    private DroolsConsumerController consumerController = new DroolsConsumerController();

    public void businessLogic(){
        consumerController.consumeEvents("group-1", -1, 10);
    }
}
