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
package org.kie.u212.core;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatcherFactory {
  private static final Logger logger = LoggerFactory.getLogger(WatcherFactory.class);


  public static Watcher createModifiedLogWatcher(String podName) {
    logger.info("Created MODIFIED watcher on pod {}", podName);
    return new Watcher<Event>() {
      @Override
      public void eventReceived(Action action,
                                Event event) {
        if (action.equals(Action.MODIFIED)) {
          logger.info("Action:{} event:{}",
                      action.name(),
                      event);
        }
      }

      @Override
      public void onClose(KubernetesClientException e) {

      }
    };
  }

    public static Watcher createAddedLogWatcher(String podName) {
      logger.info("Created {} watcher on pod {}", podName);
      return new Watcher<Event>() {
        @Override
        public void eventReceived(Action action,
                                  Event event) {
          if (action.equals(Action.ADDED)) {
            logger.info("Action:{} event:{}", action.name(), event);
          }
        }

        @Override
        public void onClose(KubernetesClientException e) {

        }
      };

  }
}
