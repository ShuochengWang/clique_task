#!/bin/bash
set -e

# compile tee_app
pushd tee_app
occlum-cargo build
occlum-cargo run
popd
