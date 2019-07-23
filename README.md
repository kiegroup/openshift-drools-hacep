## Prerequisites

- Openshift 3.11 or Minishift

- A Kafka Cluster on Openshift 3.11 with Strimzi https://strimzi.io/
(tested on Openshift 3.11 and strimzi 0.11.1)

## Creation of Kafka's topics
Create the kafka topics using the files in the kafka-topics folder, 
the cluster's name y default is "my-cluster", change accordingly in 
the yaml files with your cluster's name 
```sh
oc create -f events.yaml
oc create -f control.yaml
oc create -f snapshot.yaml
oc create -f kiesessioninfos.yaml
```
Checks the topics
```sh
oc exec -it my-cluster-kafka-0 -- bin/kafka-topics.sh --zookeeper localhost:2181 --describe
```

#### Pre deploy on Openshift
Relax RBAC for configmap
```sh
kubectl create clusterrolebinding permissive-binding --clusterrole=cluster-admin --group=system:serviceaccounts
```

## Build the pods
```sh
mvn clean install -DskipTests
```
### Deployment
Are available three modules 

- Springboot     (openshift-kie-springboot.jar 52,5 mb)
- Thorntail      (openshift-kie-thorntail.jar 111 mb)
- Jdk HttpServer (openshift-kie-jdkhttp.jar 36,9 mb)

choose your and move in the respective module to run the resepcitve command 
to create the Container image and then deploy on Openshift, as described in the module's README.md

### Client module
- sample-hacep-project-client 

#### Client configuration
From the root of the client module:
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