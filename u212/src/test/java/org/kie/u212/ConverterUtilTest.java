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
package org.kie.u212;

import static org.junit.Assert.*;
import org.junit.Test;
import org.kie.u212.model.StockTickEvent;

public class ConverterUtilTest {

    @Test
    public void serializeAndDeserializeTest(){
        StockTickEvent event = new StockTickEvent();
        event.setCompany("RHT");
        event.setPrice(10l);
        event.setTimestamp(1000l);
        byte[] bytez = ConverterUtil.serializeObj(event);
        StockTickEvent deserializedEvent = (StockTickEvent) ConverterUtil.deSerializeObj(bytez);
        assertTrue(event.getPrice() == deserializedEvent.getPrice());
        assertEquals(event.getCompany(),deserializedEvent.getCompany());
        assertTrue(event.getTimestamp() == deserializedEvent.getTimestamp());
    }

    @Test
    public void serializeAndDeserializeIntoTest(){
        StockTickEvent event = new StockTickEvent();
        event.setCompany("RHT");
        event.setPrice(10l);
        event.setTimestamp(1000l);
        ConverterUtil.addManagedType(StockTickEvent.class);
        byte[] bytez = ConverterUtil.serializeObj(event);
        StockTickEvent deserializedEvent = ConverterUtil.deSerializeObjInto(bytez, StockTickEvent.class);
        assertTrue(event.getPrice() == deserializedEvent.getPrice());
        assertEquals(event.getCompany(),deserializedEvent.getCompany());
        assertTrue(event.getTimestamp() == deserializedEvent.getTimestamp());
    }
}
