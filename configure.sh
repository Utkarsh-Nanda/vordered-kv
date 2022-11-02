#!/bin/bash

INSTALL_ROOT=$HOME/deploy
BUILD_BENCHMARKS=TRUE

# call from the build directory
cmake -DBUILD_BENCHMARKS=$BUILD_BENCHMARKS -DCMAKE_PREFIX_PATH="$HOME/deploy;$HOME/deploy/lib/cmake" -DCMAKE_INSTALL_PREFIX=$HOME/deploy ..
