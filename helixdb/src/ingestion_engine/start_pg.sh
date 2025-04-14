#!/bin/bash

# Start PostgreSQL server with specified configuration
docker run --rm \
  --name helix-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:latest
