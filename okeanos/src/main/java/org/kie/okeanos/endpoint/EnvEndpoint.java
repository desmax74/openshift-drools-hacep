package org.kie.okeanos.endpoint;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/env")
public class EnvEndpoint {

  @Path("/all")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String all() {
    StringBuilder sb = new StringBuilder();
    Map<String, String> env = System.getenv();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
    }
    return sb.toString();
  }
}
