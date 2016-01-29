#!/bin/sh
cd $TSTSRCBASE
hg files -I '{monetdb5,sql,geom}/**.{[ch],mal}' -X '**/Tests/**' | python -c 'import MonetDBtesting.malcheck'
