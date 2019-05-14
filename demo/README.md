# Demo Client event producer
Example from the root of the client-producer:
Generate a keystore and use "password" as a password
```sh
keytool -genkeypair -keyalg RSA -keystore keystore.jks
mv keystore.jks src/main/resources
```
extract the cert from openshift with:
```sh
oc extract secret/my-cluster-cluster-ca-cert --keys=ca.crt --to=- > src/main/resources/ca.crt
```
```sh
keytool -import -trustcacerts -alias root -file src/main/resources/ca.crt -keystore src/main/resources/keystore.jks -storepass password -noprompt
```

-In the configuration.properties add the path of the keystore.jks 
in the fields:
"ssl.keystore.location"
and 
"ssl.truststore.location"
in the fields
"ssl.keystore.password"
and 
"ssl.truststore.password"
the password used during the generation of the jks file

-in the field
"bootstrap.servers"
add the address of the bootstrap.servers exposed in the routes
