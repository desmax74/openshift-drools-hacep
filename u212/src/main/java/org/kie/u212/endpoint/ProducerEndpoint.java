package org.kie.u212.endpoint;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.kie.u212.producer.DroolsEventProducerApp;
import org.kie.u212.PubSubConfig;
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
        return PubSubConfig.getBotStrapServers();
    }

    @GET
    @Path("/demo/{eventNumber}")
    @Produces(MediaType.TEXT_PLAIN)
    public String demo(@PathParam("eventNumber") Integer eventNumber) {
        logger.info("Requested {} events", eventNumber);
        myEventProducerApp.businessLogic(eventNumber);
        return "produced " + eventNumber + " events";
    }

}
