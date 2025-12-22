#!/bin/bash

set -euo pipefail

printf "[!] Retrieving current configmap...\n"
startdate=$(kubectl get cm data-api-cm -n default -o jsonpath='{.data.startdate}')
enddate=$(kubectl get cm data-api-cm -n default -o jsonpath='{.data.enddate}')

printf "[!] Current date range:\n"
printf "\t- Start date: ${startdate}\n\t- End date: ${enddate}\n\n"

read -p "[*] Insert new start date (Leave empty for default): " new_startdate

if [ -z "$new_startdate" ]; then
    new_startdate=$(date -d "$enddate + 1 day" +%Y-%m-%d 2>/dev/null)
    printf "[*] New start date set to default value: ${new_startdate}\n"
fi

read -p "[*] Insert new end date (Leave empty for default): " new_enddate

if [ -z "$new_enddate" ]; then
    new_enddate=$(date -d "$new_startdate + 1 month" +%Y-%m-%d 2>/dev/null)
    printf "[*] New end date set to default value: ${new_enddate}\n"
fi

new_startdate_scs=$(date -d ${new_startdate} +%s) 2>/dev/null
last_enddate_scs=$(date -d ${enddate} +%s) 2>/dev/null

if [ -z "${new_startdate_scs}" ] || [ -z "${last_enddate_scs}" ]; then
    printf "[e] Date format is invalid!\n"
    exit 1
fi

printf "\n[*] Applying new date range... (start: ${new_startdate}, end: ${new_enddate})\n"

kubectl patch cm data-api-cm -n default --type merge -p "{\"data\": {\"startdate\": \"${new_startdate}\", \"enddate\": \"${new_enddate}\"}}"

if [ $? -eq 0 ]; then
    printf "\n[*] ConfigMap updated successfully.\n"
else
    printf "\n[e] Failed to update ConfigMap.\n"
fi
