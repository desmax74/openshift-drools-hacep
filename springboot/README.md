# Drools 

## Installing and Running
- Kafka Cluster on Openshift
https://github.com/desmax74/openshift-handbook/blob/master/fedora/kafka.md

- Prerequisites: install qs-playground 8.0-SNAPSHOT

In the root of the project
```sh
mvn clean package
```
### API

- http://localhost:8080/rest/hello

- http://<address>/rest/env/all  env vars

- http://<address>//rest/pub/user/ starts the creation of 10 events on the Kafka's topic named users
### Manual Deploy on Openshift

#### Build Container on docker
In the springboot module
```sh
docker build -t quickstarter/openshift-kie-springboot:latest .
docker images | grep openshift-kie
```

#### Deploy on Openshift
By default will be created under project called "My Project"
```sh
kubectl create -f kubernetes/deployment.yaml 
kubectl create -f kubernetes/service.yaml 
oc expose service  openshift-kie-springboot
```

 ```
 oc get route
 
  NAME           HOST/PORT                                      PATH      SERVICES       PORT      TERMINATION   WILDCARD
  openshift-kie   openshift-kie-my-kafka-project.192.168.99.109.nip.io           openshift-kie   http                    None
  ```
  
  Your address will be
  http://quick-drools-my-kafka-project.192.168.99.109.nip.io/rest/hello
  
  To see all the env available use the address
  http://quick-drools-my-kafka-project.192.168.99.109.nip.io/rest/env/all


