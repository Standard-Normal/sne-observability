#!/bin/env bash

# resources:
# - https://cloud.google.com/cloud-build/docs/deploying-builds/deploy-cloud-run#fully-managed
# - https://cloud.google.com/run/docs/configuring/environment-variables#yaml


SERVICE=sneobservability
source ../credentials/.env_vars
cp ../credentials/FRONTOFFICE-4a9964373eb2.json FRONTOFFICE-4a9964373eb2.json
cp -r ../sn-tools sn-tools
gcloud builds submit


gcloud run services update ${SERVICE} \
    --cpu 4000m\
    --memory 8192Mi \
    --update-env-vars PG_HOST=10.51.161.3,PG_USER=${PG_USER},PG_PWD=${PG_PWD},GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID} \
    --vpc-connector projects/frontoffice-291900/locations/us-west1/connectors/cloudrun-to-cloudsql \
    --timeout 900

sudo rm -r sn-tools
rm FRONTOFFICE-4a9964373eb2.json
# gcloud beta run services replace service.yaml
