#!/bin/bash

docker-compose up --build evaluate > starvers_eval.log
docker-compose down
docker rmi -f $(docker images -f "dangling=true" -q).
