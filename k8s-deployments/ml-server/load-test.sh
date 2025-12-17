#!/bin/bash

# Run this command with hey load test CLI from https://github.com/rakyll/hey
./hey -m POST -z 1m -c 50 -D test-input.json -T "application/json" http://localhost:8080/predict
