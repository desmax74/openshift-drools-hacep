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