/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kie.u212.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.kie.u212.model.StockTickEvent;
import org.kie.u212.producer.DroolsProducer;

public class DroolsEventProducerApp {

    private DroolsProducer droolsProducer;

    public DroolsEventProducerApp(){
        droolsProducer = new DroolsProducer();
    }

    public void businessLogic(Integer eventNumber){
        List<StockTickEvent> events = new ArrayList<>();
        for(int i=0; i < eventNumber; i++) {
            //@TODO add some random in these numbers: price and timestamp
            events.add(new StockTickEvent( "RHT", 100.00, 100L , UUID.randomUUID().toString()));
        }
        droolsProducer.create(events);
    }

    public void businessLogic(StockTickEvent event){

        droolsProducer.create(event);
    }
}
