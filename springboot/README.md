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

#### Build and using a local registry 

With OpenShift Container Platform 4.1, a Docker socket will not be present on the host nodes. 
This means the mount docker socket option of a custom build is not guaranteed to provide an 
accessible Docker socket for use within a custom build image.

Define the BuildConfig
```sh
oc new-build --binary --strategy=docker --name openshift-kie-springboot
```

Run the build from the dir with Dockerfile
```sh
oc start-build openshift-kie-springboot --from-dir=. --follow
```

After the build completes, your new custom builder image is available in your project in an image stream tag 
that is named openshift-kie-springboot:latest.


To see the images stream of our namespace e.g. my-kafka-project
```sh
oc get is -n my-kafka-project -o yaml
```

This return something like 
```sh
apiVersion: v1
items:
- apiVersion: image.openshift.io/v1
  kind: ImageStream
  metadata:
    annotations:
      openshift.io/generated-by: OpenShiftNewBuild
    creationTimestamp: "2019-10-15T13:30:59Z"
    generation: 1
    labels:
      build: openshift-kie-springboot
    name: openshift-kie-springboot
    namespace: my-kafka-project
    resourceVersion: "483982"
    selfLink: /apis/image.openshift.io/v1/namespaces/my-kafka-project/imagestreams/openshift-kie-springboot
    uid: 0629b104-ef50-11e9-8212-0a580a800198
  spec:
    lookupPolicy:
      local: false
  status:
    dockerImageRepository: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot
    publicDockerImageRepository: default-route-openshift-image-registry.apps-crc.testing/my-kafka-project/openshift-kie-springboot
    tags:
    - items:
      - created: "2019-10-15T14:52:52Z"
        dockerImageReference: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot@sha256:ac6d04a11bbda4a236abc65118101f16f515bcdefc7e5e86c9f6bb21cd227ca0
        generation: 1
        image: sha256:ac6d04a11bbda4a236abc65118101f16f515bcdefc7e5e86c9f6bb21cd227ca0
      tag: latest
kind: List
metadata:
  resourceVersion: ""
  selfLink: ""
```

But to Get the image URL from the image stream yaml skipping the useless information we use the Go template syntax
```sh
oc get is/openshift-kie-springboot -o template --template='{{range .status.tags}}{{range .items}}{{.dockerImageReference}}{{end}}{{end}}'
```
Open the deployment_registry yaml and replace existing image URL with the result of the previous command trimming the tail after @ symbol then add :latest. 
E.g. image: 
```sh
 - env:
   name: openshift-kie-springboot
   image: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot:latest
```
  
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
 http://<address>/rest/env/all
```