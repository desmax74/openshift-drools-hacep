package org.kie.u212.endpoint;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.kie.u212.Config;
import org.kie.u212.election.EtcdElectionCLient;
import org.kie.u212.election.EtcdElectionCLientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebListener
public class ContextListener implements ServletContextListener {

    private Logger logger = LoggerFactory.getLogger(ContextListener.class);

    public void contextInitialized(ServletContextEvent event) {
        event.getServletContext().setAttribute(Config.ETCD_CLIENT, new EtcdElectionCLientImpl());
        System.out.println("Etcd client stored in the servlet context with key:"+Config.ETCD_CLIENT);
        logger.info("Etcd client stored in the servlet context with key:{}", Config.ETCD_CLIENT);
    }

    public void contextDestroyed(ServletContextEvent event) {
        Object attribute = event.getServletContext().getAttribute(Config.ETCD_CLIENT);
        if(attribute != null){
            EtcdElectionCLient client = (EtcdElectionCLient) attribute;
            client.close();
        }
        logger.info("Etcd client stored in the servlet context closed");
    }
}
