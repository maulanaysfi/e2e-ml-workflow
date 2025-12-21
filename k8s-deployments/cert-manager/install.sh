#/bin/bash

wget https://cert-manager.io/public-keys/cert-manager-pgp-2021-09-20-1020CF3C033D4F35BAE1C19E1226061C665DF13E.asc

gpg --import cert-manager-pgp-2021-09-20-1020CF3C033D4F35BAE1C19E1226061C665DF13E.asc
gpg --export $HOME/.gnupg/pubring.gpg

rm -v cert-manager-pgp-2021-09-20-1020CF3C033D4F35BAE1C19E1226061C665DF13E.asc

helm install cert-manager jetstack/cert-manager -n cert-manager --create-namespace --set crds.enabled=true --verify
