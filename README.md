### Motivation and general overview

The purpose of this project is providing a reference architecture for High Availability Drools (especially for Complex
Event Processing scenarios) to support failover that automatically recovers from a server failure.

High availability is achieved by processing the same events on both leader and one or more replica(s). In this way, when
the leader has a failure, one of the replica can seamlessly take its place and continue the processing of new event practically
without any interruption. The election of the leader is implemented with Kubernetes ConfigMaps and its coordination with the
replicas is performed via messages exchange through Kafka. The leader is always the first to process an event and when done
notifies the replicas. A replica starts executing an event only after it has been completely processed on leader. 

When a new replica joins the cluster it asks a snapshot of the current drools session to the leader (that could produce
it on demand if there isn't a recent enough snapshot already available), deserialzes it and eventually executes the last
events not included in the snapshot before starting to process the new event in coordination with the leader.

### Complex events processing

Long running complex events processing is a very common use case for Drools deployed in a high availability architecture.
An event models a fact that happened in a specific point in time and Drools offers a rich set of temporal operators to
compare, correlate and accumulate events. For this reason every event has to have an associated timestamp assigned to it.
In a high availability environment it is strongly suggested to have this timestamp as a property of the Java bean modelling
the event. In this case, to tell the Drools engine what attribute to use as the source of the event’s timestamp is enough
to annotate the event class with @Timestamp annotation using as parameter the name of the timestamp attribute itself. As 
in the following example.

```java
@Role(Role.Type.EVENT)
@Timestamp("myTime")
public class StockTickEvent implements Serializable {

    private String company;
    private double price;
    private long myTime;
}
```

If such a timestamp attribute won't be provided Drools will by default assign to the event a timestamp corresponding to the
time when the event is inserted by the client into a remote session. In this case, if there are multiple clients inserting
events in the same session, will be necessary to synchronize the machine clocks of all the nodes hosting those clients in order 
to avoid inconsistencies when processing 2 events inserted by 2 different clients. 

### Architectural
[Diagrams](/docs/diagrams)


## Installation Guide
### Prerequisites

- Openshift 3.11, 4.X or Minishift, CRC

- A Kafka Cluster on Openshift with Strimzi https://strimzi.io/
(tested on Openshift 3.11, 4.x and strimzi 0.12.1)

### Kafka Cluster
It is necessary to have a Kafka cluster runing to use HA-CEP.
Kafka cluster can be deployed on Openshift using Strimzi operator.
For more details about instalaltion and configuration of Kafka cluster
[see](/docs/kafka-topics/README.md)


#### Implementing the HA CEP server on Openshift 4.2

The high-availability (HA) CEP server runs on the Red Hat OpenShift Container Platform environment. It includes all necessary Drools rules and other code required to process events.

You must prepare the source, build it, and then deploy it on Red Hat OpenShift Container Platform. 

1) Change to the openshift-drools-hacep-distribution/sources directory.
Review and modify the server code based on the sample project in the sample-hacep-project/sample-hacep-project-kjar directory. 
The complex event processing logic is defined by the DRL rules in the src/main/resources/org.drools.cep subdirectory.
If you want to deploy a kjar on startup or update at runtime from a maven repo [see](/docs/kjar/README.md) 

2) Build the project using the standard Maven command: 

```sh
mvn clean install -DskipTests
```
3) Use the OpenShift operator infrastructure to install [Strimzi](https://strimzi.io/) .

4) Using the KafkaTopic resource on Red Hat OpenShift Container Platform, create the topics from all the YAML files in the kafka-topics subdirectory. 
For instructions about creating topics using the KafkaTopic resource, see [Strimzi docs](https://strimzi.io/docs/latest).

5) In order to enable application access to the ConfigMap that is used in the leader election, you must configure role-based access control. 
Change to the springboot directory and enter the following commands: 
```sh
oc create -f kubernetes/service-account.yaml
oc create -f kubernetes/role.yaml
oc create -f kubernetes/role-binding.yaml
```
  
  For more information about configuring role-based access control in Red Hat OpenShift Container Platform, see [Using RBAC to define and apply permissions](https://docs.okd.io/latest/admin_guide/manage_rbac.html) in the Red Hat OpenShift Container Platform product documentation.

6) In the springboot directory, enter the following commands to build the Docker image and push it into the Docker registry that is configured on your system. (Consider configuring a private registry before running these commands). This build imports the built sample-hacep-project-kjar code as a Maven dependency and includes it in the BOOT-INF/lib directory of the openshift-kie-springboot.jar file. 
The Docker build then uses the JAR file to create the image.
```sh
docker login --username=<user username>
docker build -t <user_username>/openshift-kie-springboot:<tag> .
docker push <user_username>/openshift-kie-springboot:<tag> 
```

### Detailed docs
Detailed docs about kjar customization, kafka-topics, available modules and openshift versions

- [Docs](/docs/README.md)

  
  
  


   