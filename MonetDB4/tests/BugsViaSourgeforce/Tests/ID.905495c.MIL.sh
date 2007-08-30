#!/bin/sh

NAME="$1"

Mlog   "$MIL_CLIENT $NAME.mil"
eval    $MIL_CLIENT $NAME.mil

Mlog   "$MIL_CLIENT < $NAME.mil"
eval    $MIL_CLIENT < $NAME.mil

