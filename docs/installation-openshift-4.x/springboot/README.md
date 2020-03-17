#### Login into Openshift cluster
```sh
oc login -u kubeadmin -p <XXXXX-XXXX-XXXXX-XXXXX> https://api.crc.testing:6443'
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

##### Deploy on Openshift 4
After installed Strimzi with operator, 

create the topics with the yaml in the kafka-topics

from th UI (Kafka Topic) or from cli

#### Build and using a local registry 

With OpenShift Container Platform 4.3.X, a Docker socket will not be present on the host nodes.
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
Open the deployment yaml and replace existing image URL with the result of the previous command trimming the tail after @ symbol then add :latest. 
E.g. image: 
```sh
 - env:
   name: openshift-kie-springboot
   image: image-registry.openshift-image-registry.svc:5000/my-kafka-project/openshift-kie-springboot:latest
```

then deploy
```sh
oc create -f <path>/kubernetes/deployment.yaml
```

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
  
### REST API
```sh
 http://<address>/rest/env/all
```