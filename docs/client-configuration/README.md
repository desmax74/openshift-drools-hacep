#### Client configuration
From the root of the client module sample-hacep-project/sample-hacep-project-client:
Generate a keystore and use "password" as a password
```sh
keytool -genkeypair -keyalg RSA -keystore src/main/resources/keystore.jks
```
extract the cert from openshift with:
```sh
oc extract secret/my-cluster-cluster-ca-cert --keys=ca.crt --to=- > src/main/resources/ca.crt
```
```sh
keytool -import -trustcacerts -alias root -file src/main/resources/ca.crt -keystore src/main/resources/keystore.jks -storepass password -noprompt
```

- In the configuration.properties add the path of the keystore.jks 
in the fields:
"ssl.keystore.location"
and 
"ssl.truststore.location"
in the fields
"ssl.keystore.password"
and 
"ssl.truststore.password"
the passwords used during the generation of the jks file and the trustore

- in the field
"bootstrap.servers" add the address of the bootstrap.servers exposed in the routes
