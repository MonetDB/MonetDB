#!/bin/sh

q=compilation_or_runtime_choice.SF-1991738_0.xq
p="${PF-pf}"
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
