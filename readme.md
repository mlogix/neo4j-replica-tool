Launch work environment in docker container:

```docker run --rm -it -v "$PWD":/usr/src/ne04j-replica-tool -w /usr/src/ne04j-replica-tool golang:1.12 bash```

Build code: `./build.sh`
