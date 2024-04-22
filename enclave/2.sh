#!/bin/bash
set -e

# # compile tee_app
# pushd tee_app
# occlum-cargo build
# popd

# # initialize occlum workspace
# rm -rf occlum_instance && mkdir occlum_instance && cd occlum_instance

# occlum init && rm -rf image
cd occlum_instance
copy_bom -f ../rust.yaml --root image --include-dir /opt/occlum/etc/template

SGX_MODE=SIM occlum build
OCCLUM_LOG_LEVEL=info occlum run /bin/tee_app
