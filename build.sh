#!/usr/bin/env bash

# wget https://github.com/neo4j-drivers/seabolt/releases/download/v1.7.4/seabolt-1.7.4-Linux-ubuntu-18.04.tar.gz
#      https://github.com/neo4j-drivers/seabolt/releases/download/v1.7.4/seabolt-1.7.4-win64-mingw.zip
#      https://github.com/neo4j-drivers/seabolt/releases/download/v1.7.4/seabolt-1.7.4-Darwin.tar.gz

# tar zxvf seabolt-1.7.4-Linux-ubuntu-18.04.tar.gz --strip-components=1 -C /
# rm -f seabolt-1.7.4-Linux-ubuntu-18.04.tar.gz

for GOOS in darwin linux windows; do
  for GOARCH in 386 amd64 arm arm64; do
    export GOOS GOARCH
    output_name=bin/neo4j-replica-tool-$GOOS-$GOARCH
    if [ $GOOS = "windows" ]; then
        output_name+='.exe'
    fi
    echo "Build for ${GOOS} : ${GOARCH}"
    go build -v -o $output_name
  done
done
