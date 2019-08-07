#### Login into Openshift cluster
```sh
oc login -u system:admin
eval $(minishift oc-env) 
eval $(minishift docker-env)
oc project my-kafka-project
```

#### Relax RBAC for configmap
```sh
kubectl create clusterrolebinding permissive-binding --clusterrole=cluster-admin --group=system:serviceaccounts
```

#### Build Container on docker
In the jdkhttp module.

note:The name of the image "quickstarter/openshift-kie-jdkhttp"
could be changed accordingly with the name used as image in the file kubernetes/deployment.yaml

```sh
docker build -t quickstarter/openshift-kie-jdkhttp:latest .
docker images | grep openshift-kie
```

By default will be created under project called "My Project"
```sh
kubectl create -f kubernetes/deployment.yaml 
kubectl create -f kubernetes/service.yaml 
oc expose service  openshift-kie-jdkhttp
```

 ```
 oc get route
 
  NAME           HOST/PORT                                      PATH      SERVICES       PORT      TERMINATION   WILDCARD
  openshift-kie   openshift-kie-my-kafka-project.192.168.99.109.nip.io           openshift-kie   http                    None
  ```
  
  Your address will be
  http://openshift-kie-jdkhttp-my-kafka-project.192.168.99.109.nip.io/rest/env/all
  
  
#### Build Container and deploy with fabric8
In the jdkhttp module
##### Build docker image
```sh
mvn package docker:build
```
##### Deploy
```sh
mvn fabric8:resource fabric8:deploy
```  
  
### Remote debug    
    
#### Using docker hub registry
```sh
docker login --username=<user username>
docker build -t <user_username>/openshift-kie-jdkhttp:<tag> .  
docker push <user_username>/openshift-kie-jdkhttp:<tag>
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
 http://<address>/rest/env/all
```