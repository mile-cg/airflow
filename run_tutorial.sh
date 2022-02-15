#!/bin/zsh

gcloud composer environments storage dags import --environment stg-composer-2  --location us-central1 --source dag/quickstart.py