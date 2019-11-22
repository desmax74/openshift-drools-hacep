### Kafka cluster

- A Kafka Cluster on Openshift with Strimzi [strimzi](https://strimzi.io/)
(tested on Openshift 3.11, 4.x and strimzi 0.12.1)

### Creation of Kafka's topics
Create the kafka topics using the files in the kafka-topics folder of the module used (springboot/jdkhttp), 
the cluster's name default is "my-cluster", change it accordingly in 
the yaml files with your cluster's name 

##### Kafka's Topics
```sh
oc create -f kafka-topisc/events.yaml
oc create -f kafka-topisc/control.yaml
oc create -f kafka-topisc/snapshot.yaml
oc create -f kafka-topisc/kiesessioninfos.yaml
```

These topics must run in a single partition because is the way to deliver the same message to all consumer.
- Events is the topic where are delivered msgs from clients.
- Control is the topic with the msg processed from the events topic by the leader.
- Snapshot is the topic (compact) with the Session Snapshots.
- KiSessioninfos is the topic with the runtime answers about Session's informations.  

Checks the topics
```sh
oc exec -it my-cluster-kafka-<number> -- bin/kafka-topics.sh --zookeeper localhost:2181 --describe
```

Delete a topic
```sh
oc exec -it my-cluster-kafka-0 -- bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic <topic_name>
```

Checks Topic's offset
```sh
oc exec -it my-cluster-kafka-0 -- bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic <topic_name>
``` 