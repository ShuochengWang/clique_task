#!/bin/bash

docker run -it \
    --name occlum \
    --mount type=bind,source=/home/stuart/code,target=/root/code \
    occlum/occlum:latest-ubuntu20.04