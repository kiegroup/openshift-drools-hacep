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
``````

#### Build Container on docker
In the thorntail module.

note:The name of the image "quickstarter/openshift-kie-thorntail"
could be changed accordingly with the name used as image in the file kubernetes/deployment.yaml

Feel free to change base image in the Dockerfile if you need
```sh
docker build -t quickstarter/openshift-kie-thorntail:latest .
docker images | grep openshift-kie
```


By default will be created under project called "My Project"
```sh
kubectl create -f kubernetes/deployment.yaml 
kubectl create -f kubernetes/service.yaml 
oc expose service  openshift-kie-thorntail
```

 ```
 oc get route
 
  NAME           HOST/PORT                                      PATH      SERVICES       PORT      TERMINATION   WILDCARD
  openshift-kie   openshift-kie-my-kafka-project.192.168.99.109.nip.io           openshift-kie   http                    None
  ```
    
  Your address will be
  http://openshift-kie-thorntail-my-kafka-project.192.168.99.109.nip.io/rest/env
  
#### Build Container and deploy with fabric8
In the thorntail module
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

of kubernetes/deployment_registry.yaml

changing the image with the name of your docker image.  
  
### Remote debug    
    
#### Using docker hub registry
```sh
docker login --username=<user username>
docker build -t <user_username>/openshift-kie-springboot:<tag> .  
docker push <user_username>/openshift-kie-springboot:<tag>
```

#### Deploy
Change the image name with your image name in the following files before run the create command
```sh
kubectl create -f kubernetes/debug_pod.yaml
kubectl create -f kubernetes/deployment_registry.yaml
```

#### Port forward
port forwarding 
```sh
oc port-forward <POD> 8000 3000 3001
```
```sh
jdb -connect com.sun.jdi.SocketAttach:hostname=localhost,port=8000
```

#### Visualvm
visualvm --openjmx localhost:3000

#### IntellijIdea
Attach to process

### REST API
```sh
 http://<address>/rest/env
```