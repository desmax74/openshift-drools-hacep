#### Login into Openshift cluster
```sh
oc login -u kubeadmin -p <XXXXX-XXXX-XXXXX-XXXXX> https://api.crc.testing:6443'
oc project my-kafka-project
```
```
#### RBAC for configmap
```sh
oc create -f kubernetes/service-account.yaml
oc create -f kubernetes/role.yaml
oc create -f kubernetes/role-binding.yaml
```
#### Build Container and deploy
In the jdkhttp module.

##### Deploy on Openshift 4
After installed AMQ Streams/Strimzi with operator, 

create the topics with the yaml in the kafka-topics

from th UI (Kafka Topic) or from cli

then build the docker image and push into docker hub  
```sh
docker login --username=<user username>
docker build -t <user_username>/openshift-kie-jdkhttp:<tag> .  
docker push <user_username>/openshift-kie-jdkhttp:<tag>
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
oc new-build --binary --strategy=docker --name openshift-kie-jdkhttp
```

Edit the Dockerfile and assign a value to the placeholder <id_group> and <id_user>
```sh
RUN groupadd -r app -g <id_group> && useradd -u <id_user> -r -g app -m -d /app -s /sbin/nologin -c "App user" app && chmod 755 /app
```
before to build the docker container.

To find the range of the ids run the command
```sh
oc describe project <your project name>
```
The output will be something like this with id ranges for groups and user
```sh
oc describe project my-kafka-project
Name:			my-kafka-project
Created:		7 days ago
Labels:			<none>
Annotations:		openshift.io/description=
			openshift.io/display-name=
			openshift.io/requester=system:admin
			openshift.io/sa.scc.mcs=s0:c13,c2
			openshift.io/sa.scc.supplemental-groups=1000160000/10000
			openshift.io/sa.scc.uid-range=1000160000/10000
Display Name:		<none>
Description:		<none>
Status:			Active
Node Selector:		<none>
Quota:			<none>
Resource limits:	<none>
```

Run the build from the dir with Dockerfile
```sh
oc start-build openshift-kie-jdkhttp --from-dir=. --follow
```

After the build completes, your new custom builder image is available in your project in an image stream tag 
that is named openshift-kie-jdkhttp:latest.


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
      build: openshift-kie-jdkhttp
    name: openshift-kie-jdkhttp
    namespace: my-kafka-project
    resourceVersion: "483982"
    selfLink: /apis/image.openshift.io/v1/namespaces/my-kafka-project/imagestreams/openshift-kie-jdkhttp
    uid: 0629b104-ef50-11e9-8212-0a580a800198
  spec:
    lookupPolicy:
      local: false
  status:
    dockerImageRepository: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-jdkhttp
    publicDockerImageRepository: default-route-openshift-image-registry.apps-crc.testing/my-kafka-project/openshift-kie-jdkhttp
    tags:
    - items:
      - created: "2019-10-15T14:52:52Z"
        dockerImageReference: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-jdkhttp@sha256:ac6d04a11bbda4a236abc65118101f16f515bcdefc7e5e86c9f6bb21cd227ca0
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
oc get is/openshift-kie-jdkhttp -o template --template='{{range .status.tags}}{{range .items}}{{.dockerImageReference}}{{end}}{{end}}'
```
Open the deployment yaml and replace existing image URL with the result of the previous command trimming the tail after @ symbol then add :latest. 
E.g. image: 
```sh
 - env:
   name: openshift-kie-jdkhttp
   image: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-jdkhttp:latest
```
and replace in the deployment.yaml the id_user used in the dockerfile
```yaml
securityContext:
        runAsUser: <id_user>
        runAsNonRoot: true
```
before to deploy
  
### REST API
```sh
 http://<address>/rest/env/all
```