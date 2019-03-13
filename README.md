# Drools 

## Installing and Running
- Kafka Cluster on Openshift
https://github.com/desmax74/openshift-handbook/blob/master/fedora/kafka.md

- Prerequisites: install qs-playground 8.0-SNAPSHOT
```sh
mvn clean install
mvn thorntail:run
```
### Hello World

Point to http://localhost:8080/rest/hello for Drools hello world

### More complex example

- post 

```sh
curl -d '{"name":"edo", "age":32}' -H "Content-Type: application/json" \
    -X POST http://localhost:8080/persons                                                                                                    ~
```

- http://<address>/rest/env/all returns a list of all env vars

- http://<address>//rest/pub/demo/<items> produces N items in the kafka's topic 

- http://<address>//rest/sub/demo/ start a consumer of the Kafka's topic
### Manual Deploy on Openshift

#### Build Container on docker
```sh
docker build -t quickstarter/openshift-kie-thorntail:latest .
docker images | grep openshift-kie
```

#### Deploy on Openshift
By default will be created under project called "My Project"
```sh
kubectl create -f kubernetes/deployment.yaml 
kubectl create -f kubernetes/service.yaml 
oc expose service  openshift-kie-thorntail
```
Relax RBAC for configmap
```sh
kubectl create clusterrolebinding permissive-binding --clusterrole=cluster-admin --group=system:serviceaccounts
```

this create a yaml file and the route for us on openshift, like this (in routes section on My Project)
 ```yaml
 
 apiVersion: route.openshift.io/v1
 kind: Route
 metadata:
   annotations:
     openshift.io/host.generated: 'true'
   creationTimestamp: '2019-02-20T10:25:59Z'
   labels:
     app: openshift-kie
   name: openshift-kie
   namespace: myproject
   resourceVersion: '30743'
   selfLink: /apis/route.openshift.io/v1/namespaces/myproject/routes/openshift-kie
   uid: ea2676d6-34f9-11e9-bd97-08002709a920
 spec:
   host: openshift-kie-myproject.192.168.99.109.nip.io
   port:
     targetPort: http
   to:
     kind: Service
     name: openshift-kie
     weight: 100
   wildcardPolicy: None
 status:
   ingress:
     - conditions:
         - lastTransitionTime: '2019-02-20T10:25:59Z'
           status: 'True'
           type: Admitted
       host: openshift-kie-myproject.192.168.99.109.nip.io
       routerName: router
       wildcardPolicy: None

 ```
 ```
 oc get route
 
  NAME           HOST/PORT                                      PATH      SERVICES       PORT      TERMINATION   WILDCARD
  openshift-kie   openshift-kie-myproject.192.168.99.109.nip.io           openshift-kie   http                    None
  ```
  
  Your address will be
  http://quick-drools-myproject.192.168.99.109.nip.io/rest/hello
  
  To see all the env available use the address
  http://quick-drools-myproject.192.168.99.109.nip.io/rest/env


