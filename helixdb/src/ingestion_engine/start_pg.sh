#!/bin/bash

docker run --rm \
    --name helix-postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=helixdb \
    -p 5432:5432 \
    -v "$(pwd)/init.sql:/init.sql" \
    ankane/pgvector:latest
