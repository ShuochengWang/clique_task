#!/bin/bash

docker run -it \
    --name neo4j \
    --publish=7474:7474 --publish=7687:7687 \
    --env-file .env \
    --volume=$HOME/code/clique_task/neo4j/data:/data \
    --volume=$HOME/code/clique_task/neo4j/log:/log \
    neo4j