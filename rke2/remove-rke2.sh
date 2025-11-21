#!/bin/bash

rke2-uninstall.sh
sleep 15
rm -rfv /var/lib/calico/ /var/run/calico/ /var/run/istio-cni/ /var/run/k3s/ /var/run/ztunnel/
