#!/usr/bin/env bash

Mlog -x "$SQL_CLIENT -e < $TSTSRCDIR/../monetdb/$TST.sql"
