package org.kie.okeanos;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.PartitionInfo;

//For demo purpose
public class MyEventConsumerApp {

    private   ConsumerController consumerController = new ConsumerController();

    public void businessLogic(){
        consumerController.consumeEvents("group-1", -1, 10);
    }

    public Map<String, List<PartitionInfo>> getTopics(){
        return consumerController.getTopics();
    }
}
