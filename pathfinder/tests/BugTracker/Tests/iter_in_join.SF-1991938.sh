#!/usr/bin/env bash

q=iter_in_join.SF-1991938.xq
p='pf -M'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
p='pf -A'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
