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
must be removed from your maven module,
some other configuration are described in the following steps

##### The prerequisites
To have an updatable Kjar at startup and later, is mandatory to add two env vars 
at the startup time and the presence of the specific jar in a Maven repo. 
Accordingly to your used module change 
[springboot module](/springboot/kubernetes/deployment.yaml)
or
[jdkhttp module](/jdkhttp/kubernetes/deployment.yaml)
```yaml
containers:
        - env:
          - name: UPDATABLEKJAR
            value: "true"
          - name: KJARGAV
            value: "org.kie:sample-hacep-project:7.30.0.Final" 
```
and add the env vars
UPDATABLEKJAR with value "true"
and the desidered GAV of the KJar to use at the start up, then add to the yaml ,
the needed env vars to configure the settings.xml using the following variables.

| Name                | Description                                                  |  Example            |
|---------------------| ------------------------------------------------------------ |---------------------|
|MAVEN\_LOCAL\_REPO   | Directory to use as the local Maven repository.              | /root/.m2/repository                                                         |
|MAVEN\_MIRROR\_URL   | The base URL of a mirror used for retrieving artifacts. | http://nexus3-my-kafka-project.192.168.99.133.nip.io/repository/maven-public/|
|MAVEN\_MIRRORS       | If set, multi-mirror support is enabled, and other MAVEN\_MIRROR\_\* variables will be prefixed. For example: DEV\_ONE\_MAVEN\_MIRROR\_URL and QE\_TWO\_MAVEN\_MIRROR\_URL  | dev-one,qe-two
|MAVEN\_REPOS         |If set, multi-repo support is enabled, and other MAVEN\_REPO\_\* variables will be prefixed. For example: DEV\_ONE\_MAVEN\_REPO\_URL and QE\_TWO\_MAVEN\_REPO\_URL |dev-one,qe-two
|MAVEN\_SETTINGS\_XML |Location of custom Maven settings.xml file to use. | /root/.m2/settings.xml
|prefix\_MAVEN\_MIRROR\_ID |ID to be used for the specified mirror.  If ommitted, a unique ID will be generated. |internal-mirror
|prefix\_MAVEN\_MIRROR\_OF |Repository IDs mirrored by this entry.  Defaults to external:\* |
|prefix\_MAVEN\_MIRROR\_URL |The URL of the mirror. |http://10.0.0.1:8080/repository/internal
|prefix\_MAVEN\_REPO\_HOST |Maven repository host (if not using fully defined url; will fallback to service) |repo.example.com
|prefix\_MAVEN\_REPO\_ID |Maven repository id |my-repo
|prefix\_MAVEN\_REPO\_LAYOUT |Maven repository layout |default
|prefix\_MAVEN\_REPO\_PASSPHRASE |Maven repository passphrase |maven1!
|prefix\_MAVEN\_REPO\_PASSWORD |Maven repository password |maven1!
|prefix\_MAVEN\_REPO\_PATH |Maven repository path (if not using fully defined url; will fallback to service) |/maven2/
|prefix\_MAVEN\_REPO\_PORT |Maven repository port (if not using fully defined url; will fallback to service) |8080
|prefix\_MAVEN\_REPO\_PRIVATE\_KEY |Maven repository private key |${user.home}/.ssh/id\_dsa
|prefix\_MAVEN\_REPO\_PROTOCOL |Maven repository protocol (if not using fully defined url; will fallback to service) |http
|prefix\_MAVEN\_REPO\_RELEASES\_ENABLED |Maven repository releases enabled |true
|prefix\_MAVEN\_REPO\_RELEASES\_UPDATE\_POLICY |Maven repository releases update policy |always
|prefix\_MAVEN\_REPO\_SERVICE |Maven repository service to lookup if prefix\_MAVEN\_REPO\_URL not specified |buscentr-myapp
|prefix\_MAVEN\_REPO\_SNAPSHOTS\_ENABLED        |Maven repository snapshots enabled |true
|prefix\_MAVEN\_REPO\_SNAPSHOTS\_UPDATE\_POLICY |Maven repository snapshots update policy |always
|prefix\_MAVEN\_REPO\_URL                       |Maven repository url (fully defined) |http://repo.example.com:8080/maven2/
|prefix\_MAVEN\_REPO\_USERNAME                  |Maven repository username |mavenUser
                                                        
The kjar must be present in the Maven/Nexus repo accessed by  Aether before the start, 
Aether will ask the jar to Maven/Nexus and the jar will be placed inside maven local repo 
and loaded by Drools at the startup and on every UpdateCommand jar.