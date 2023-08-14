#!/usr/bin/env bash
set -eu

cd "$(dirname "$0")"

docker run \
    -d \
    --rm \
    --name zipkin \
    -p 9411:9411 \
    openzipkin/zipkin:latest
