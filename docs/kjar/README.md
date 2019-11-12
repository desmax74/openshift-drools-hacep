### Kjar

#### Deploy kjar at startup from the fat jar

This is the default configuration using the sample-hacep-project/sample-hacep-project-kjar maven module, 
no env vars are required but it can't be updated at runtime.

#### Deploy kjar at startup from Maven Repo

To enable the update at startup and at runtime some ENV VArs are needed in the deployment.yaml
and the dependency 
```xml
<dependency>
      <groupId>org.kie</groupId>
      <artifactId>sample-hacep-project-kjar</artifactId>
</dependency>
```
must be removed from you maven module,
some other configuration are described in the following steps

##### The prerequisites
To have an updatable Kjar at startup and later, is mandatory to add two env vars 
at the startup time and the presence of the specific jar in a Nexus repo. 

Install a Nexus sonatype , and provide a maven-group to
group together your maven repos needed by your project,
copy the Nexus url, something like:
```sh
http://nexus3-my-kafka-project.192.168.99.133.nip.io/repository/maven-public/
```
replace the NEXUS_URL in the sprinboot/settings.xml with this url, something like:

```xml
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
and in this way the local cahced maven repo will be 
```sh 
/opt/.m2/repository
```
Now we need to create a storage mounted as /opt/.m2/repository by the pods
at the startup time.

Create a storage called 'maven-repo' with write and read many access mode

In the deployment.yaml we add
the volume mount
```yaml
volumeMounts:
            - mountPath: /opt/.m2/repository
              name: maven-repo
          securityContext:
            privileged: false
```
Add the volume
```yaml
volumes:
        - name: maven-repo
          persistentVolumeClaim:
            claimName: maven-repo
            privileged: false
```

and add the env vars
UPDATABLEKJAR with value "true"
and the desidered GAV of the KJar to use at the start up:

```yaml
containers:
        - env:
          - name: UPDATABLEKJAR
            value: "true"
          - name: KJARGAV
            value: "org.kie:sample-hacep-project:7.29.0-SNAPSHOT" 
```

At the end the deployment,yaml will be
```yaml
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
          image: quickstarter/openshift-kie-springboot:latest
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
Aether will ask the jar to NExus and the jar will be putted inside /opt/.m2/repository 
and loaded by Drools at the startup and on every UpdateCommand jar.

