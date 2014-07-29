#!/usr/bin/env bash

TST=${TST/_bam}
$TSTSRCDIR/Test.SQL.sh ${*/_bam}
