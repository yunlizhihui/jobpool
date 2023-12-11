#!/usr/bin/env bash

source ./scripts/test_lib.sh

echo "start ----"
DIRS="./api ./client ./server ./jobpoolctl ./tools/mod ./agent"
for dir in ${DIRS}; do
  run pushd "${dir}"
    run go mod tidy
    run go mod vendor
  run popd
done
echo "finish mod all the modules ----"
