#!/bin/sh

NAME="$1"

Mlog -x "$MIL_CLIENT $NAME.mil"

Mlog -x "$MIL_CLIENT < $NAME.mil"

