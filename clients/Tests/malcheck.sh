#!/bin/sh
cd $TSTSRCBASE
hg locate -I monetdb5 -I geom -I sql -X '**/Tests/**' '*.[ch]' '*.mal' | python -c 'import MonetDBtesting.malcheck'
