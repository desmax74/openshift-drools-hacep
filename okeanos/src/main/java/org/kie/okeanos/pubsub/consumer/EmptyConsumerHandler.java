package org.kie.okeanos.pubsub.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyConsumerHandler implements ConsumerHandler {

    private Logger logger = LoggerFactory.getLogger(EmptyConsumerHandler.class);

    @Override
    public void process(ConsumerRecord record) {
        logger.info("Process:{}",record);
    }
}
