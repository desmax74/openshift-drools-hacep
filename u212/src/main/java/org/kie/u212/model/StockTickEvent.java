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

package org.kie.u212.model;

import org.kie.api.definition.type.Role;
import org.kie.api.definition.type.Timestamp;

@Role(Role.Type.EVENT)
@Timestamp( "timestamp" )
public class StockTickEvent {

    private final String company;
    private final double price;
    private final long timestamp;
    private final String id;

    public StockTickEvent(String company, double price, long timestamp, String id ) {
        this.company = company;
        this.price = price;
        this.timestamp = timestamp;
        this.id = id;
    }

    public String getCompany() {
        return company;
    }

    public double getPrice() {
        return price;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getId() { return id; }
}
