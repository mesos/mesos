#!/bin/sh

# Set local Mesos runner to use 3 slaves
export MESOS_NUM_SLAVES=3

# Check that the C++ test framework executes without crashing (returns 0).
exec $MESOS_BUILD_DIR/src/test-framework local
