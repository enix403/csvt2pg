#!/usr/bin/env bash

# docker run --name csv2pg-store -p 5432:5432 -e POSTGRES_PASSWORD=pass postgres

docker container stop csv2pg-store
docker container rm csv2pg-store

docker run -d \
  --rm -it \
  --name csv2pg-store \
  -v ./postgres-data:/var/lib/postgresql/data \
  -p 8554:5432 \
  -e POSTGRES_USER=user1 \
  -e POSTGRES_PASSWORD=user1 \
  postgres:latest
