##########################
# deploy section
##########################

kubectl patch configmap/inferenceservice-config -n kserve --type=strategic -p '{"data": {"deploy": "{\"defaultDeploymentMode\": \"RawDeployment\"}"}}'

##########################
# ingress section
##########################

kubectl patch configmap/inferenceservice-config -n kserve --type=strategic -p '{"data": {"ingress": "{\"enableGatewayApi\": false, \"ingressGateway\": \"kube-system/rke2-ingress-nginx-controller-admission\", \"ingressClassName\": \"nginx\", \"disableIngressCreation\": false, \"ingressDomain\": \"model-api.maulanaysfi.my.id\"}"}}' 
