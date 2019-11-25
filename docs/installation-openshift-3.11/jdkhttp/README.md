#### Login into Openshift cluster
```sh
oc login -u system:admin
eval $(minishift oc-env) 
eval $(minishift docker-env)
oc project my-kafka-project
```
#### RBAC for configmap
```sh
oc create -f kubernetes/service-account.yaml
oc create -f kubernetes/role.yaml
oc create -f kubernetes/role-binding.yaml
```
#### Build Container and deploy
In the jdkhttp module.

note:The name of the image "quickstarter/openshift-kie-jdkhttp"
could be changed accordingly with the name used as image in the file kubernetes/deployment.yaml

Feel free to change base image in the Dockerfile if you need
```sh
docker build -t quickstarter/openshift-kie-jdkhttp:latest .
docker images | grep openshift-kie
```

By default will be created under project called "My Project"
```sh
kubectl create -f kubernetes/deployment.yaml 
kubectl create -f kubernetes/service.yaml 
oc expose service  openshift-kie-jdkhttp
```

 ```sh
 oc get route
 
  NAME           HOST/PORT                                      PATH      SERVICES       PORT      TERMINATION   WILDCARD
  openshift-kie   openshift-kie-my-kafka-project.192.168.99.109.nip.io           openshift-kie   http                    None
  ```
    
  Your address will be
  http://openshift-kie-jdkhttp-my-kafka-project.192.168.99.109.nip.io/rest/env/all
  
  
#### Build Container and deploy with fabric8
In the springboot module
##### Build docker image
```sh
mvn package docker:build
```
##### Deploy
```sh
mvn fabric8:resource fabric8:deploy
```
  
### REST API
```sh
 http://<address>/rest/env/all
```