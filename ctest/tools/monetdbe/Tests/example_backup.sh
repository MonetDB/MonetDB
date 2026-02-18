#!/bin/sh
f=$(mktemp)
trap "rm -f $f" EXIT
example_backup $f > /dev/null
