#!/bin/bash

kubectl create secret generic s3-secret -n kserve --from-env-file=.env 
