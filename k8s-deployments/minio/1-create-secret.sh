#!/bin/bash

if kubectl get namespace minio >/dev/null 2>&1; then
  echo "Namespace minio exists."
else
  echo "Namespace minio does not exist. Creating new minio namespace."
  kubectl create namespace minio
fi

echo "Creating minio secret."
kubectl create secret generic minio-root-credentials -n minio --from-env-file=.env
