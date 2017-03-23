#!/bin/bash

CONT_NAME=$1
docker inspect -f '{{.Config.Hostname}}' $CONT_NAME

docker stop $CONT_NAME && docker rm $CONT_NAME
