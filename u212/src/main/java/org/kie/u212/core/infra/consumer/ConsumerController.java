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
package org.kie.u212.core.infra.consumer;

import org.kie.u212.Config;

public class ConsumerController {

  private Restarter restarter;

  public ConsumerController(Restarter bag) {
    this.restarter = bag;
  }

  public void consumeEvents() {
    Thread t = new Thread(
            new ConsumerThread(
                    Config.DEFAULT_POLL_SIZE,
                    Config.LOOP_DURATION,
                    Config.DEFAULT_COMMIT_SYNC,
                    true,
                    restarter));
    t.start();
  }
}
