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
