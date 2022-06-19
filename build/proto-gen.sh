#!/usr/bin/env bash

set -e

root="$(pwd)"
src_dir=${root}
imports=(
  "${src_dir}/api"
  "${src_dir}/third-party"
 )

function cleanup() {
    local rc=$?
    if [[ -d ${root}/.tmp ]]; then
      rm -rf ${root}/.tmp
    fi
    exit ${rc}
}

trap cleanup INT TERM EXIT

# set proto path
protoc_arg=""
for i in "${imports[@]}"
do
  protoc_arg+="--proto_path=$i "
done

# generate code from API proto
mkdir -p ${root}/.tmp/gen
for p in `find "${root}/api" -maxdepth 2 -mindepth 2 -type d`; do
  echo Generating ${p} protobuf code ...
  files="$(find ${p} -type f -name "*.proto")"
  protoc \
    ${protoc_arg} \
    --go_out=${root}/.tmp/gen \
    --go-grpc_out=${root}/.tmp/gen \
    --grpc-gateway_out=${root}/.tmp/gen \
    ${files[@]}
done

cp -r ${root}/.tmp/gen/appmeta/api ${root}
