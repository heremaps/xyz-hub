#! /bin/bash
# This script pushes image defined in Dockerfile to container registry defined in envs below

# populate env vars
. naksha_psql_image.conf

# decide whether use docker or podman
if command -v docker &> /dev/null
then
    CONTAINER_TOOL=docker
elif command -v podman &> /dev/null;
then
    CONTAINER_TOOL=podman
else
    echo "none of ['docker', 'podman'] commands could be found"
    exit 1
fi
echo "Using $CONTAINER_TOOL as container management tool"

# build image & push it to the container
IMAGE_ID=$CONTAINER_REPOSITORY/$IMAGE_NAMESPACE/$IMAGE_NAME:$IMAGE_VERSION
$CONTAINER_TOOL build -t "$IMAGE_ID" --platform=linux/amd64 .
$CONTAINER_TOOL push "$IMAGE_ID"
