#!/bin/bash

curl -sfL https://get.rke2.io | sh -
mkdir -p /etc/rancher/rke2/
cat <<EOF > /etc/rancher/rke2/config.yaml
cluster-cidr: 10.51.0.0/16
service-cidr: 10.52.0.0/16
cluster-dns: 10.52.0.10
tls-san: cluster1
EOF

systemctl enable rke2-server.service --now

mkdir -p /home/ibrahim/.kube
cp -v /etc/rancher/rke2/rke2.yaml /home/ibrahim/.kube/config
chown -v ibrahim:ibrahim -R /home/ibrahim/.kube
