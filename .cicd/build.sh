#!/bin/bash
set -e
export IMAGE="history-tools:$(git rev-parse HEAD)"
echo '+++ :docker: Building Container'
cd docker
DOCKER_BUILD="docker build --build-arg CDT_BRANCH -f ubuntu-18.04.dockerfile -t $IMAGE ."
echo "$ $DOCKER_BUILD"
eval $DOCKER_BUILD
echo '+++ :package: Create rodeos Installer'
[[ -z "$PACKAGE_REVISION" ]] && export PACKAGE_REVISION='1'
[[ -z "$VERSION_STRING" ]] && export VERSION_STRING="$(docker run -it "$IMAGE" rodeos --version || : | sed 's/^v//' | sed "s/$/-$PACKAGE_REVISION/")"
DOCKER_RUN="docker run -e BUILDKITE -e PACKAGE_REVISION -e VERSION_STRING -w /eosio.cdt/libraries/history-tools -t \"$IMAGE\" ./scripts/package-rodeos.sh"
echo "$ $DOCKER_RUN"
eval $DOCKER_RUN
echo '+++ :arrow_up: Uploading Artifacts'
echo '$ docker ps -l'
docker ps -l
CONTAINER_ID="$(docker ps -l | tail -1 | awk '{print $1}')"
echo "Retrieving rodeos installer from container $CONTAINER_ID."
DOCKER_CP="docker cp $CONTAINER_ID:/eosio.cdt/libraries/history-tools/build/rodeos_$VERSION_STRING.deb ."
echo "$ $DOCKER_CP"
eval $DOCKER_CP
echo 'Uploading rodeos installer to s3://rodeos-binaries/ci...'
buildkite-agent artifact upload '*.deb' 's3://rodeos-binaries/ci'