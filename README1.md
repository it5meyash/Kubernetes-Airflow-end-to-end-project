Kubernetes for Data Engineering
installing kubernetes and running on dokcer
installing helm(The package manager for Kubernetes)
used to conncet to dashboard ----kubectl proxy 
for accessing dashboard --------http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#/error?namespace=default 
for getting k8s token value ------kubectl get secret admin-user -n kubernetes-dashboard -o jsonpath={".data.token"} | base64 -d
