#!/usr/bin/env bash

q=slow_compilation.SF-2898944.q1.xq
p='pf -M'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
p='pf -A'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null

q=slow_compilation.SF-2898944.q2.xq
p='pf -M'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
p='pf -A'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
