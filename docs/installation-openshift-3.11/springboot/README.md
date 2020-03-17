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
  
#### Configure log level
Log level can be configured using pod environment variables. Environment variables are
specified in section spec -> template -> spec -> containers -> env in deployment.yaml.
Following environment variables can be used for log level configuration:

| Property name | Description |
| --- | --- |
| HACEP_LOG_LEVEL | Log level of HA-CEP components |
| KAFKA_LOG_LEVEL | Log level of Kafka |
| KIE_SCANNER_LOG_LEVEL | Log level of Kie scanner |
| ROOT_LOG_LEVEL | Log level of other components |
| SPRING_LOG_LEVEL | Log level of Spring |
  
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