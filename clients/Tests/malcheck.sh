#!/bin/sh
cd $TSTSRCBASE
hg -q files -I '{monetdb5,sql,geom}/**.{[ch],mal}' -X '**/Tests/**' | python -c 'import MonetDBtesting.malcheck'
