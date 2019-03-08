package org.kie.okeanos;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kie.okeanos.pubsub.consumer.PubSubConfig;
import org.kie.okeanos.model.MyEvent;
import org.kie.okeanos.pubsub.producer.EventProducer;
import org.kie.okeanos.pubsub.utils.RecordMetadataUtil;

public class ProducerController {

    public ProducerController(){ }

    public RecordMetadata create(List<MyEvent> events) {
        EventProducer<MyEvent>  eventProducer = new EventProducer<>();
        eventProducer.start(PubSubConfig.getDefaultConfig());
        RecordMetadata lastRecord = null;
        for(MyEvent event: events) {
            lastRecord = eventProducer.produceSync(new ProducerRecord<>(PubSubConfig.MASTER_TOPIC,
                                                                                       event.getId(),
                                                                                       event));
            RecordMetadataUtil.logRecord(lastRecord);
        }
        eventProducer.stop();
        return lastRecord;
    }


}
