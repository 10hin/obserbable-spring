#!/usr/bin/env bash
set -eu

cd "$(dirname "$0")"

docker run \
    -d \
    --rm \
    --name mosquitto \
    -p 1883:1883 \
    -v "${PWD}/mosquitto.conf":/mosquitto/config/mosquitto.conf \
    eclipse-mosquitto:2
