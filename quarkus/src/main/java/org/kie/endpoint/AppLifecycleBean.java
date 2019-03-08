package org.kie.endpoint;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class AppLifecycleBean {

  private static final Logger logger = LoggerFactory.getLogger("ListenerBean");

  void onStart(@Observes StartupEvent ev) {
    logger.info("The application is starting...");
  }

  void onStop(@Observes ShutdownEvent ev) {
    logger.info("The application is stopping...");
  }

}