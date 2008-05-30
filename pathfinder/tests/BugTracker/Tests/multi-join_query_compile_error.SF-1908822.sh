#!/usr/bin/env bash

q=multi-join_query_compile_error.SF-1908822.OK.xq
p='pf -M'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
p='pf -A'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null

q=multi-join_query_compile_error.SF-1908822.KO.xq
p='pf -M'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
p='pf -A'
echo "\"$p $q >/dev/null\""
echo "\"$p $q >/dev/null\"" >&2
$p $q >/dev/null
