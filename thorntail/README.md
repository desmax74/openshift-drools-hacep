# Drools 

## Installing and Running
- Kafka Cluster on Openshift
https://github.com/desmax74/openshift-handbook/blob/master/fedora/kafka.md

- Prerequisites: install qs-playground 8.0-SNAPSHOT
```sh
mvn clean package
```

### API

- http://localhost:8080/rest/hello

- http://<address>/rest/env/all  env vars

- http://<address>//rest/pub/user/ startz the creation of 10 events on the Kafka's topic named users

### Manual Deploy on Openshift

#### Build Container on docker
In the thorntail module
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


