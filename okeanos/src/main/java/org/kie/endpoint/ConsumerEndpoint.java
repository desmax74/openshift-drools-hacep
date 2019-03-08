package org.kie.endpoint;

import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.common.PartitionInfo;
import org.kie.okeanos.MyEventConsumerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/sub")
public class ConsumerEndpoint {

    private Logger logger = LoggerFactory.getLogger(ProducerEndpoint.class);
    private static MyEventConsumerApp myEventConsumerApp= new MyEventConsumerApp();

    @GET
    @Path("/demo")
    @Produces(MediaType.TEXT_PLAIN)
    public String demo() {
        myEventConsumerApp.businessLogic();
        return "started 1 consumer";
    }

    @GET
    @Path("/topics")
    @Produces(MediaType.TEXT_PLAIN)
    public String topics() {
        logger.info("Topics");
        Map<String, List<PartitionInfo>> topics = myEventConsumerApp.getTopics();
        StringBuilder sb = new StringBuilder();
        for(Map.Entry entry :topics.entrySet()){
            sb.append("Key:").append(entry.getKey()).append(":]");
            List<PartitionInfo> itemValues = (List<PartitionInfo>)entry.getValue();
            for(PartitionInfo info: itemValues){
                sb.append(info.toString());
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
