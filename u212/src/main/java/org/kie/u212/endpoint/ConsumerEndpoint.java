package org.kie.u212.endpoint;


import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.kie.u212.consumer.DroolsEventCosumerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/sub")
public class ConsumerEndpoint {

    private Logger logger = LoggerFactory.getLogger(ProducerEndpoint.class);
    private static DroolsEventCosumerApp myEventConsumerApp= new DroolsEventCosumerApp();

    @GET
    @Path("/demo")
    @Produces(MediaType.TEXT_PLAIN)
    public String demo() {
        myEventConsumerApp.businessLogic();
        return "started 1 consumer";
    }

}
