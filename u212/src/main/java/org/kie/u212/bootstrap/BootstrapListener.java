package org.kie.u212.bootstrap;

import javax.servlet.ServletContextEvent;
import javax.servlet.annotation.WebListener;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.kie.u212.Config;
import org.kie.u212.election.KubernetesLockConfiguration;
import org.kie.u212.election.Leadership;
import org.kie.u212.election.LeadershipImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebListener
public class BootstrapListener {

    private Logger logger = LoggerFactory.getLogger(BootstrapListener.class);

    public void contextInitialized(ServletContextEvent event) {
        KubernetesLockConfiguration configuration = new KubernetesLockConfiguration();
        KubernetesClient client = new DefaultKubernetesClient();
        event.getServletContext().setAttribute(Config.ELECTION_SERVICE, new LeadershipImpl(client,configuration));
        logger.info("ElectionService stored in the servlet context with key:{}", Config.ELECTION_SERVICE);
    }

    public void contextDestroyed(ServletContextEvent event) throws Exception {
        Object attribute = event.getServletContext().getAttribute(Config.ELECTION_SERVICE);
        if(attribute != null){
            Leadership service = (Leadership) attribute;
            service.stop();;
        }
        logger.info("Etcd client stored in the servlet context closed");
    }
}
