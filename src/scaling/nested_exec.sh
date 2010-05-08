#!/bin/sh
export PYTHONPATH="$PYTHONPATH:`dirname $0`/../swig/python"
exec `dirname $0`/nested_exec.py $@
