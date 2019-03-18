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
package org.kie.u212.endpoint;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.kie.u212.Config;
import org.kie.u212.producer.DroolsEventProducerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/pub")
public class ProducerEndpoint {

  private static DroolsEventProducerApp myEventProducerApp = new DroolsEventProducerApp();

  private Logger logger = LoggerFactory.getLogger(ProducerEndpoint.class);

  @GET
  @Path("/brokers")
  @Produces(MediaType.TEXT_PLAIN)
  public String brokers() {
    return Config.getBotStrapServers();
  }

  @GET
  @Path("/user")
  @Produces(MediaType.TEXT_PLAIN)
  public String user() {
    logger.info("Requested {} events topic:users", 10);
    myEventProducerApp.businessLogic(10, "users");
    return "produced " + 10 + " events";
  }

  @GET
  @Path("/master")
  @Produces(MediaType.TEXT_PLAIN)
  public String master() {
    logger.info("Requested {} events topic:master", 10);
    myEventProducerApp.businessLogic(10, "master");
    return "produced " + 10 + " events";
  }


}
