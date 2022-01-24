#!/bin/sh
cd $TSTSRCBASE
hg -q files -I '{monetdb5,sql,geom}/**.[ch]' -X '**/Tests/**' | python -c 'import MonetDBtesting.melcheck'
