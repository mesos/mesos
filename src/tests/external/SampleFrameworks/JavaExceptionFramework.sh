#!/bin/sh

# Check that the JavaException framework crashes and prints an
# ArrayIndexOutOfBoundsExcpetion. This is a test to be sure that Java
# exceptions are getting propagated. Th exit status of grep should be 0.

$MESOS_BUILD_DIR/src/examples/java/test-exception-framework local 2>&1 \
  | grep "ArrayIndexOutOfBoundsException"

exit $?
