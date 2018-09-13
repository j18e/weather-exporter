#!/bin/bash -eu

REMOTE_REPO='jamesmacfarlane'
IMAGE="${PWD##*/}"
TAG="$(git rev-parse --short HEAD)"

docker build -t ${IMAGE}:${TAG} .
docker tag ${IMAGE}:${TAG} ${IMAGE}:latest
docker tag ${IMAGE}:${TAG} ${REMOTE_REPO}/${IMAGE}:${TAG}
docker tag ${IMAGE}:${TAG} ${REMOTE_REPO}/${IMAGE}:latest

#docker push ${REMOTE_REPO}/${IMAGE}:${TAG}
#docker push ${REMOTE_REPO}/${IMAGE}:latest

echo "${REMOTE_REPO}/${IMAGE}:${TAG}"

