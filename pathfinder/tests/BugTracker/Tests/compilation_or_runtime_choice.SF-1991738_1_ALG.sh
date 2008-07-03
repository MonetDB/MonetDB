#!/usr/bin/env bash

q=compilation_or_runtime_choice.SF-1991738_1_ALG.xq
p='pf -A'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
