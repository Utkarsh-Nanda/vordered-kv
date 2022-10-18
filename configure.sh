#!/bin/bash

# call from the build directory
cmake -DCMAKE_PREFIX_PATH="$HOME/deploy/lib/cmake;$HOME/deploy" ..
