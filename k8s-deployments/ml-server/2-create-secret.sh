#!/bin/bash

kubectl create secret generic s3-secret --from-env-file=.envrc 
