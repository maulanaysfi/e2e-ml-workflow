#!/bin/bash

kubectl create secret generic data-api-s3-credentials -n default --from-env-file=.env