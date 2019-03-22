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

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.kie.u212.Config;
import org.kie.u212.producer.DroolsEventProducerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Endpoints {

  private Logger logger = LoggerFactory.getLogger(Endpoints.class);

  @GetMapping("/env/all")
  public ResponseEntity<String> all() {
    StringBuilder sb = new StringBuilder();
    Map<String, String> env = System.getenv();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
    }
    return ResponseEntity.status(HttpStatus.OK).body(sb.toString());
  }

  @GetMapping("/health")
  public ResponseEntity<String> check() {
    return ResponseEntity.status(HttpStatus.OK).build();
  }


  private static DroolsEventProducerApp myEventProducerApp = new DroolsEventProducerApp();


  @GetMapping("/brokers")
  public String brokers() {
    return Config.getBotStrapServers();
  }


  @GetMapping("/pub/user")
  public String user() {
    logger.info("Requested {} events topic:users", 10);
    myEventProducerApp.businessLogic(10, Config.EVENTS_TOPIC);
    return "produced " + 10 + " Config.EVENTS_TOPIC";
  }
}
