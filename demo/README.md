# Demo
After configured the keystore in the client module
configure the path and the ip in the ClientProducerTest
```
    props.put("bootstrap.servers", "my-cluster-kafka-bootstrap-my-kafka-project.<ip>.nip.io:443");
    props.put("security.protocol", "SSL");
    props.put("ssl.keystore.location", "/<path>/openshift-drools-hacep/client/src/main/resources/keystore.jks");
    props.put("ssl.keystore.password", "password");
    props.put("ssl.truststore.location", "/<path>/openshift-drools-hacep/client/src/main/resources/keystore.jks");
    props.put("ssl.truststore.password", "password");
```
