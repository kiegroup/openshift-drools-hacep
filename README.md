### Motivation and general overview

The purpose of this project is providing a reference architecture for High Availability Drools (especially for Complex
Event Processing scenarios) to support failover that automatically recovers from a server failure.

High availability is achieved by processing the same events on both leader and one or more replica(s). In this way, when
the leader has a failure, one of the replica can seamlessly take its place and continue the processing of new event practically
without any interruption. The election of the leader is implemented with Kubernetes ConfigMaps and its coordination with the
replicas is performed via messages exchange through Kafka. The leader is always the first to process an event and when done
notifies the replicas. A replica starts executing an event only after it has been completely processed on leader. 

When a new replica joins the cluster it asks a snapshot of the current drools session to the leader (that could produce
it on demand if there isn't a recent enough snapshot already available), deserialzes it and eventually executes the last
events not included in the snapshot before starting to process the new event in coordination with the leader.

### Complex events processing

Long running complex events processing is a very common use case for Drools deployed in a high availability architecture.
An event models a fact that happened in a specific point in time and Drools offers a rich set of temporal operators to
compare, correlate and accumulate events. For this reason every event has to have an associated timestamp assigned to it.
In a high availability environment it is strongly suggested to have this timestamp as a property of the Java bean modelling
the event. In this case, to tell the Drools engine what attribute to use as the source of the event’s timestamp is enough
to annotate the event class with @Timestamp annotation using as parameter the name of the timestamp attribute itself. As 
in the following example.

```java
@Role(Role.Type.EVENT)
@Timestamp("myTime")
public class StockTickEvent implements Serializable {

    private String company;
    private double price;
    private long myTime;
}
```

If such a timestamp attribute won't be provided Drools will by default assign to the event a timestamp corresponding to the
time when the event is inserted by the client into a remote session. In this case, if there are multiple clients inserting
events in the same session, will be necessary to synchronize the machine clocks of all the nodes hosting those clients in order 
to avoid inconsistencies when processing 2 events inserted by 2 different clients. 

### Architectural (doc folder)
@TODO with images

### Customization (kjar)
@TODO

## Installation Guide
### Prerequisites

- Openshift 3.11 or Minishift

- A Kafka Cluster on Openshift 3.11 with Strimzi https://strimzi.io/
(tested on Openshift 3.11 and strimzi 0.11.1 and 0.12.1)

### Creation of Kafka's topics
Create the kafka topics using the files in the kafka-topics folder, 
the cluster's name default is "my-cluster", change it accordingly in 
the yaml files with your cluster's name 
##### TODO describe each topic in terms of configuration and single partition reason
```sh
oc create -f kafka-topisc/events.yaml
oc create -f kafka-topisc/control.yaml
oc create -f kafka-topisc/snapshot.yaml
oc create -f kafka-topisc/kiesessioninfos.yaml
```
Checks the topics
```sh
oc exec -it my-cluster-kafka-<number> -- bin/kafka-topics.sh --zookeeper localhost:2181 --describe
```

### Pre deploy on Openshift
Relax Role based access control (RBAC https://kubernetes.io/docs/reference/access-authn-authz/rbac/) for configmap
@TODO change with one less powerful or with a different strategy
```sh
kubectl create clusterrolebinding permissive-binding --clusterrole=cluster-admin --group=system:serviceaccounts
```

### Build the pods
```sh
mvn clean install -DskipTests
```
### Deployment
Are available three modules 

- Springboot     ( openshift-kie-springboot.jar )
- Thorntail      ( openshift-kie-thorntail.jar )
- Jdk HttpServer ( openshift-kie-jdkhttp.jar )

choose your and move in the respective module to run the resepcitve command 
to create the Container image and then deploy on Openshift, as described in the module's README.md


### Client outside cluster

If you plan to use a client outside openshift
you need to expose kafka with a route
in the kafka cluster creation you could enable the https endpoint with
listener external of type route

```json
apiVersion: kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    version: 2.2.1
    replicas: 3
    listeners:
      plain: {}
      external:
        type: route
```


### Client module
- sample-hacep-project-client 

#### Client configuration
From the root of the client module:
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
