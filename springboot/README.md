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
In the springboot module.

note:The name of the image "quickstarter/openshift-kie-springboot"
could be changed accordingly with the name used as image in the file kubernetes/deployment.yaml

Feel free to change base image in the Dockerfile if you need
```sh
docker build -t quickstarter/openshift-kie-springboot:latest .
docker images | grep openshift-kie
```

By default will be created under project called "My Project"
```sh
kubectl create -f kubernetes/deployment.yaml 
kubectl create -f kubernetes/service.yaml 
oc expose service  openshift-kie-springboot
```

 ```sh
 oc get route
 
  NAME           HOST/PORT                                      PATH      SERVICES       PORT      TERMINATION   WILDCARD
  openshift-kie   openshift-kie-my-kafka-project.192.168.99.109.nip.io           openshift-kie   http                    None
  ```
    
  Your address will be
  http://openshift-kie-springboot-my-kafka-project.192.168.99.109.nip.io/rest/env/all
  
  
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

##### Deploy on Openshift 4
After installed AMQ Streams/Strimzi with operator, 

create the topics with the yaml in the kafka-topics

from th UI (Kafka Topic) or from cli

then build the docker image and push into docker hub  
```sh
docker login --username=<user username>
docker build -t <user_username>/openshift-kie-springboot:<tag> .  
docker push <user_username>/openshift-kie-springboot:<tag>
```
then create a yaml from the UI using the content 

of kubernetes/deployment.yaml

changing the image with the name of your docker image.

#### Build and using a local registry 

With OpenShift Container Platform 4.1, a Docker socket will not be present on the host nodes. 
This means the mount docker socket option of a custom build is not guaranteed to provide an 
accessible Docker socket for use within a custom build image.

Define the BuildConfig
```sh
oc new-build --binary --strategy=docker --name openshift-kie-springboot
```

Run the build from the dir with Dockerfile
```sh
oc start-build openshift-kie-springboot --from-dir=. --follow
```

After the build completes, your new custom builder image is available in your project in an image stream tag 
that is named openshift-kie-springboot:latest.


To see the images stream of our namespace e.g. my-kafka-project
```sh
oc get is -n my-kafka-project -o yaml
```

This return something like 
```sh
apiVersion: v1
items:
- apiVersion: image.openshift.io/v1
  kind: ImageStream
  metadata:
    annotations:
      openshift.io/generated-by: OpenShiftNewBuild
    creationTimestamp: "2019-10-15T13:30:59Z"
    generation: 1
    labels:
      build: openshift-kie-springboot
    name: openshift-kie-springboot
    namespace: my-kafka-project
    resourceVersion: "483982"
    selfLink: /apis/image.openshift.io/v1/namespaces/my-kafka-project/imagestreams/openshift-kie-springboot
    uid: 0629b104-ef50-11e9-8212-0a580a800198
  spec:
    lookupPolicy:
      local: false
  status:
    dockerImageRepository: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot
    publicDockerImageRepository: default-route-openshift-image-registry.apps-crc.testing/my-kafka-project/openshift-kie-springboot
    tags:
    - items:
      - created: "2019-10-15T14:52:52Z"
        dockerImageReference: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot@sha256:ac6d04a11bbda4a236abc65118101f16f515bcdefc7e5e86c9f6bb21cd227ca0
        generation: 1
        image: sha256:ac6d04a11bbda4a236abc65118101f16f515bcdefc7e5e86c9f6bb21cd227ca0
      tag: latest
kind: List
metadata:
  resourceVersion: ""
  selfLink: ""
```

But to Get the image URL from the image stream yaml skipping the useless information we use the Go template syntax
```sh
oc get is/openshift-kie-springboot -o template --template='{{range .status.tags}}{{range .items}}{{.dockerImageReference}}{{end}}{{end}}'
```
Open the deployment yaml and replace existing image URL with the result of the previous command trimming the tail after @ symbol then add :latest. 
E.g. image: 
```sh
 - env:
   name: openshift-kie-springboot
   image: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot:latest
```
  
### REST API
```sh
 http://<address>/rest/env/all
```


### Update KJar at startup

#### The prerequisites
To have an updatable Kjar at startup and later, is mandatory to add two env vars 
at the startup time and the presence of the specific jar in a Nexus repo. 

Install a Nexus sonatype , and provide a maven-group to
group together your maven repos needed by your project,
copy the Nexus url, something like:
```sh
http://nexus3-my-kafka-project.192.168.99.133.nip.io/repository/maven-public/
```
replace the NEXUS_URL in the sprinboot/settings.xml with this url, something like:

```sh
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <localRepository>/opt/.m2/repository</localRepository>
  <pluginGroups></pluginGroups>
  <proxies></proxies>
  <servers></servers>
  <mirrors>
    <mirror>
      <id>central</id>
      <mirrorOf>central</mirrorOf>
      <name>nexus</name>
      <url>http://nexus3-my-kafka-project.192.168.99.133.nip.io/repository/maven-public/</url>
    </mirror>
  </mirrors>
  <profiles></profiles>
</settings>
```

this file will be copied in the
```sh 
/root/.m2/settings.xml 
```
by the Dockerfile
```sh 
COPY target/*-springboot.jar /deployments/app.jar
COPY settings.xml /root/.m2/settings.xml
EXPOSE 8080
```
and in this way the local cached maven repo will be 
```sh 
/opt/.m2/repository
```
Now we need to create a storage mounted as /opt/.m2/repository by the pods
at the startup time.
 
Create a storage called 'maven-repo' with write and read many access mode

In the deployment.yaml we add
the volume mount
```sh
volumeMounts:
            - mountPath: /opt/.m2/repository
              name: maven-repo
          securityContext:
            privileged: false
```
Add the volume
```sh
volumes:
        - name: maven-repo
          persistentVolumeClaim:
            claimName: maven-repo
            privileged: false
```

and add the env vars
UPDATABLEKJAR with value "true"
and the desidered GAV of the KJar to use at the start up:

```sh
containers:
        - env:
          - name: UPDATABLEKJAR
            value: "true"
          - name: KJARGAV
            value: "org.kie:sample-hacep-project:7.29.0-SNAPSHOT" 
```

At the end the deployment,yaml will be
```sh
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: openshift-kie-springboot
    version: v1
  name: openshift-kie-springboot
spec:
  replicas: 3
  selector:
    matchLabels:
      app: openshift-kie-springboot
      version: v1
  template:
    metadata:
      labels:
        app: openshift-kie-springboot
        version: v1
    spec:
      serviceAccountName: openshift-kie-springboot
      containers:
        - env:
          - name: UPDATABLEKJAR
            value: "true"
          - name: KJARGAV
            value: "org.kie:sample-hacep-project:7.29.0-SNAPSHOT"
          name: openshift-kie-springboot
          image: <user>/openshift-kie-springboot:<version>
          imagePullPolicy: IfNotPresent
          livenessProbe:
            exec:
              command:
                - curl
                - localhost:8080/liveness
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 1
          ports:
            - containerPort: 8080
              name: http
              protocol: TCP
          readinessProbe:
            exec:
              command:
                - curl
                - localhost:8080/readiness
            initialDelaySeconds: 10
            periodSeconds: 5
            timeoutSeconds: 1
          volumeMounts:
            - mountPath: /opt/.m2/repository
              name: maven-repo
          securityContext:
            privileged: false
      volumes:
        - name: maven-repo
          persistentVolumeClaim:
            claimName: maven-repo          
```

the kjar must be present in the nexus repo accessed by our Aether before the start, 
Aether will ask the jar to Nexus and the jar will be putted inside /opt/.m2/repository 
and loaded by Drools at the startup and on every UpdateCommand jar.