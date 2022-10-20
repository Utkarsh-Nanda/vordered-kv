#!/bin/bash

INSTALL_ROOT=$HOME/deploy
# call from the build directory
cmake -DCMAKE_PREFIX_PATH="$HOME/deploy;$HOME/deploy/lib/cmake" -DCMAKE_INSTALL_PREFIX=$HOME/deploy ..
