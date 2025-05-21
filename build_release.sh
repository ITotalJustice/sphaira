#!/bin/sh

# builds a preset
build_preset() {
    echo Configuring $1 ...
    cmake --preset $1
    echo Building $1 ...
    cmake --build --preset $1
}

build_preset MinSizeRel

rm -rf out

# --- SWITCH --- #
mkdir -p out/switch/sphaira/
cp -r build/MinSizeRel/*.nro out/switch/sphaira/sphaira.nro
pushd out
zip -r9 sphaira.zip switch
popd
