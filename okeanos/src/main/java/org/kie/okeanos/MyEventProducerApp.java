package org.kie.okeanos;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.kie.okeanos.model.MyEvent;

//For demo purpose
public class MyEventProducerApp {

    private ProducerController producerController;

    public MyEventProducerApp(){
        producerController = new ProducerController();
    }

    public void businessLogic(Integer eventNumber){
        List<MyEvent> events = new ArrayList<>();
        for(int i=0; i < eventNumber; i++) {
            events.add(new MyEvent("ID-" + UUID.randomUUID().toString(), "Name-" + UUID.randomUUID().toString()));
        }
        producerController.create(events);
    }
}
