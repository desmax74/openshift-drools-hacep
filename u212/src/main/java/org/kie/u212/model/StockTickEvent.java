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
@Timestamp("timestamp")
public class StockTickEvent {

  private String company;
  private double price;
  private long timestamp;

  public StockTickEvent(String company,
                        double price) {
    this.company = company;
    this.price = price;
  }

  public StockTickEvent(){ }

  public void setCompany(String company) {
    this.company = company;
  }

  public void setPrice(double price) {
    this.price = price;
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

  public void setTimestamp(long timestamp) {
    if (this.timestamp == 0) {
      this.timestamp = timestamp;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("StockTickEvent{");
    sb.append("company='").append(company).append('\'');
    sb.append(", price=").append(price).append('\'');
    sb.append(", timestamp=").append(timestamp);
    sb.append('}');
    return sb.toString();
  }
}
